CC          = g++
CFLAGS      = -O3 -g -pthread -std=c++14

CCHEADER   = btree.h
CCSOURCE   = btree.cc main.cc
EXECBIN     = btree
OBJECTS     = ${CCSOURCE:.cc=.o}

all : ${EXECBIN}

${EXECBIN} : ${OBJECTS}
	${CC} ${CFLAGS} -o $@ $^
    
%.o : %.cc
	${CC} ${CFLAGS} -c $<

.PHONY : clean

clean :
	rm ${OBJECTS} ${EXECBIN}
