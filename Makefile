ab:
	ab -p bench.txt -c 5 -n 50000 http://127.0.0.1:8000/queue/foo

ab_osx:
	sudo sysctl -w net.inet.tcp.msl=15000
	sudo sysctl -w net.inet.tcp.msl=100
	ab -p bench.txt -c 5 -n 50000 http://127.0.0.1:8000/queue/foo
	sudo sysctl -w net.inet.tcp.msl=15000


run_profile:
	rm -rf cqueue.stacks
	cargo build --release
	sudo dtrace -c "./target/release/cqueue" -o cqueue.stacks -n 'profile-997 /execname == "cqueue"/ { @[ustack(100)] = count(); }'

flame:
	rm -rf /tmp/graph.svg
	/Users/tim/proj_/pl/FlameGraph/stackcollapse.pl cqueue.stacks | /Users/tim/proj_/pl/FlameGraph/flamegraph.pl > /tmp/graph.svg
	open /tmp/graph.svg