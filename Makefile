export GOPROXY=goproxy.cn,direct

OBJ=kafka-consumer
all: $(OBJ)

$(OBJ):
	go build -o $@ ./

clean:
	rm -fr $(OBJ)

-include .deps

dep:
	echo -n "$(OBJ):" > .deps
	find . -path ./vendor -prune -o -name '*.go' -print | awk '{print $$0 " \\"}' >> .deps
	echo "" >> .deps
