#!/usr/bin/expect -f
spawn scp sriva147@csel-kh4250-11.cselabs.umn.edu:/PROJECT3.tar ~
expect "password: "
send "Password\r"