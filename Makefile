CC = gcc
CFLAGS = -g -Wall -Wno-missing-braces -std=gnu99 #-O2 -DNDEBUG
LDFLAGS = -pthread -lrt

CLIENT_EXE = client
CLIENT_SRC = client.c md5.c util.c

MSERVER_EXE = mserver
MSERVER_SRC = mserver.c util.c

SERVER_EXE = server
SERVER_SRC = server.c util.c hash.c

TARGETS = CLIENT MSERVER SERVER

CLEAN_FILES = *.log
CLEAN_DIRS = util util/collections

$(foreach t, $(TARGETS), $(eval $t_OBJ = $($t_SRC:.c=.o)))

ALL_EXE = $(foreach t, $(TARGETS), $($t_EXE))
ALL_OBJ = $(foreach t, $(TARGETS), $($t_OBJ))

.PHONY: all clean

all: $(ALL_EXE)

$(foreach t, $(TARGETS), $(eval $($t_EXE): $($t_OBJ); $(CC) $(LDFLAGS) $$^ -o $$@))

-include $(ALL_OBJ:.o=.d)

%.o: %.c
	$(CC) $(CFLAGS) -c -MMD $< -o $@

clean:
	rm -f $(ALL_EXE) *.o *.d *~ $(CLEAN_FILES) $(foreach d, $(CLEAN_DIRS), $d/*.o $d/*.d)
