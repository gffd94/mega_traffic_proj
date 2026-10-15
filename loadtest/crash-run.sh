#!/usr/bin/env bash
# 장애 주입 측정 (Day 12-13): 부하 도중 앱을 강제 종료하거나 RabbitMQ를 재시작하고, 끝난 뒤 장부와 근거를 모은다.
#
# 실행 (프로젝트 루트, Git Bash. 앱은 이 스크립트가 직접 띄우므로 미리 띄워 두지 않는다):
#   loadtest/crash-run.sh all                  # docker compose down -v → up -d 후 아래 5회를 차례로
#   loadtest/crash-run.sh app <run> <초>       # 부하 시작 <초> 뒤 앱 강제 종료(taskkill /F), DOWN초 뒤 재기동
#   loadtest/crash-run.sh mq <run> <초>        # 부하 시작 <초> 뒤 docker restart mega-rabbitmq
#
# 환경 변수: RATE(20), DURATION(90), DOWN(10, 앱 강제 종료 후 재기동까지 초)
# 필요: java 21, k6, node(크래시 도구), docker. 앱 jar는 ./gradlew bootJar로 미리 빌드한다.
#
# 결과: loadtest/results/crash-{app|mq}-run{N}.json (k6 결과 + crash-tool.mjs analyze가 덧붙인 근거)
# 근거 원본(앱 로그, k6 로그, 장애 순간 스냅샷): build/crash-logs/{app|mq}-run{N}/

set -uo pipefail
cd "$(dirname "$0")/.."
set -a
. ./.env
set +a

RATE=${RATE:-20}
DURATION=${DURATION:-90}
DOWN=${DOWN:-10}
JAR=build/libs/mega-order-0.0.1-SNAPSHOT.jar
LOG_ROOT=build/crash-logs
TOOL="node loadtest/crash-tool.mjs"

# 지금 떠 있는 앱의 Windows PID와 그 앱의 로그 파일
APP_PID=""
APP_LOG=""

now() { date +%s%3N; }

sleep_until() {
	local wait_ms=$(($1 - $(now)))
	if [ "$wait_ms" -gt 0 ]; then
		sleep "$(awk "BEGIN { print $wait_ms / 1000 }")"
	fi
}

wait_app_ready() {
	for _ in $(seq 1 480); do
		[ "$(curl -s -o /dev/null -w '%{http_code}' localhost:8080/actuator/health)" = 200 ] && return 0
		sleep 0.25
	done
	echo "앱이 120초 안에 뜨지 않았습니다" >&2
	return 1
}

start_app() {
	APP_LOG=$1
	# 출력을 파일로 돌리면 콘솔 코드 페이지(MS949)로 써지므로 UTF-8로 고정한다 (crash-tool.mjs가 로그를 검색)
	java -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8 -jar "$JAR" > "$APP_LOG" 2>&1 &
	APP_PID=$(cat /proc/$!/winpid)
	wait_app_ready
}

kill_app() {
	taskkill //F //PID "$APP_PID" > /dev/null
}

# 이번 실행 구간의 앱 로그만 잘라 둔다 (앱 하나가 여러 실행에 걸쳐 떠 있을 수 있어서)
cut_app_log() {
	local offset=$1 out=$2
	tail -c +"$((offset + 1))" "$APP_LOG" > "$out"
}

wait_load_start() {
	local log=$1
	for _ in $(seq 1 600); do
		local line
		line=$(grep -m1 '^LOAD_START ' "$log" 2>/dev/null) && { echo "$line" | awk '{ print $2 }'; return 0; }
		sleep 0.1
	done
	echo "k6가 60초 안에 부하를 시작하지 않았습니다" >&2
	return 1
}

run_one() {
	local mode=$1 run=$2 at=$3
	local dir=$LOG_ROOT/$mode-run$run
	rm -rf "$dir"
	mkdir -p "$dir"
	local timing=$dir/timing.env
	echo "===== $mode run$run (부하 시작 ${at}초 뒤 장애) ====="

	if [ -z "$APP_PID" ]; then
		start_app "$dir/app-0.log"
	fi
	local log_offset
	log_offset=$(stat -c %s "$APP_LOG")
	{
		echo "mode=$mode"
		echo "faultAfterSeconds=$at"
		echo "publishInBefore=$($TOOL publish-in)"
	} > "$timing"

	k6 run --log-format=raw -e RATE="$RATE" -e DURATION="$DURATION" -e MODE="$mode" -e RUN="$run" \
		loadtest/order-saga-crash.js > "$dir/k6.log" 2>&1 &
	local k6_pid=$!
	local load_start
	load_start=$(wait_load_start "$dir/k6.log") || { kill "$k6_pid"; return 1; }
	echo "loadStart=$load_start" >> "$timing"

	if [ "$mode" = app ]; then
		sleep_until $((load_start + at * 1000))
		local kill_at
		kill_at=$(now)
		kill_app
		echo "killAt=$kill_at" >> "$timing"
		echo "  $(date +%T) 앱 강제 종료 (PID $APP_PID)"
		cut_app_log "$log_offset" "$dir/app-1.log"
		# 죽은 앱의 연결이 끊기면 브로커가 미확인(unacked) 메시지를 곧바로 큐에 되돌린다. 그 뒤의 큐·DB를 찍는다
		sleep 1
		echo "  큐: $($TOOL queues "$dir/snap-queues.json" processed)"
		echo "  Outbox: $($TOOL outbox-ready "$dir/snap-outbox-ready.json")"
		sleep_until $((kill_at + DOWN * 1000))
		echo "restartAt=$(now)" >> "$timing"
		start_app "$dir/app-2.log"
		echo "readyAt=$(now)" >> "$timing"
		echo "  $(date +%T) 앱 재기동 완료 (PID $APP_PID)"
	else
		# 재시작 직전 큐 상태: 메시지 수(rabbitmqctl)와 메시지 eventId(관리 API로 꺼내 보고 되돌림)
		sleep_until $((load_start + at * 1000 - 3000))
		echo "  큐(직전): $($TOOL counts "$dir/queues-before.json")"
		$TOOL queues "$dir/snap-queues.json" > /dev/null
		sleep_until $((load_start + at * 1000))
		echo "mqRestartAt=$(now)" >> "$timing"
		echo "  $(date +%T) RabbitMQ 재시작"
		docker restart mega-rabbitmq > /dev/null
		until docker exec mega-rabbitmq rabbitmqctl await_startup > /dev/null 2>&1; do sleep 0.2; done
		echo "mqUpAt=$(now)" >> "$timing"
		echo "  큐(직후): $($TOOL counts "$dir/queues-after.json")"
		echo "consumersBackAt=$($TOOL wait-consumers)" >> "$timing"
		echo "  $(date +%T) 모든 큐에 컨슈머 다시 붙음"
	fi

	wait "$k6_pid"
	echo "k6Exit=$?" >> "$timing"
	if [ "$mode" = mq ]; then
		cut_app_log "$log_offset" "$dir/app-1.log"
	fi
	# 관리 API 통계는 5초마다 갱신된다. k6 teardown이 이미 12초 기다렸으므로 바로 읽는다
	echo "publishInAfter=$($TOOL publish-in)" >> "$timing"
	sed -n '/=====/,$p' "$dir/k6.log" | head -40
	$TOOL analyze "$mode" "$run" "$dir" > "$dir/analyze.json"
	echo "  분석: $dir/analyze.json"
}

reset_infra() {
	echo "===== docker compose down -v → up -d ====="
	docker compose down -v
	docker compose up -d
	until [ "$(docker inspect -f '{{.State.Health.Status}}' mega-mysql)" = healthy ]; do sleep 1; done
	until docker exec mega-rabbitmq rabbitmqctl await_startup > /dev/null 2>&1; do sleep 1; done
}

case "${1:-}" in
	all)
		[ -f "$JAR" ] || { echo "$JAR 없음. ./gradlew bootJar 먼저" >&2; exit 1; }
		mkdir -p "$LOG_ROOT"
		reset_infra
		run_one app 1 25 && sleep 15
		run_one app 2 35 && sleep 15
		run_one app 3 45 && sleep 15
		run_one mq 1 30 && sleep 15
		run_one mq 2 30
		kill_app
		;;
	app | mq)
		mkdir -p "$LOG_ROOT"
		run_one "$1" "$2" "$3"
		kill_app
		;;
	*)
		sed -n '2,15p' "$0"
		exit 2
		;;
esac
