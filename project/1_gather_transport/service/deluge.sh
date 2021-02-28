#!/bin/sh
# /usr/local/bin/deluge.sh 
 
function d_start() 
{ 
	echo  "Deluge: starting service" 
	deluged --pidfile = /tmp/deluge.pid
	sleep  5 
	echo  "PID is $(cat /tmp/deluge.pid) " 
}
 
function d_stop() 
{ 
	echo  "Deluge: stopping Service (PID = $(cat /tmp/deluge.pid) )" 
	kill $( cat /tmp/deluge.pid ) 
	rm  /tmp/deluge.pid
 }
 
function d_status() 
{ 
	ps  -ef  |  grep deluged |  grep  -v  grep 
	echo  "PID indicate indication file $(cat /tmp/deluge.pid 2> /dev/null) " 
}
 
# Some Things That run always 
touch  /var/lock/deluge
 
# Management instructions of the service 
case  "$1"  in 
	start)
		d_start
		;; 
	stop)
		d_stop
		;; 
	reload)
		d_stop
		sleep  1
		d_start
		;; 
	status)
		d_status
		;; 
	*) 
	Echo  "Usage: $0 {start | stop | reload | status}" 
	exit  1 
	;; 
esac
 
exit  0
