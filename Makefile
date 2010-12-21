include $(GOROOT)/src/Make.inc

TARG=govclock
GOFMT=gofmt -spaces=true -tabindent=false -tabwidth=4

GOFILES=\
	govclock.go\

include $(GOROOT)/src/Make.pkg

format:
	${GOFMT} -w govclock.go
	${GOFMT} -w govclock_test.go
