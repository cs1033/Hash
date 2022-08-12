CPP:=g++ 
FLUSH_FLAG :=-DCLWB 
CFLAGS:=-Iinclude -Iindex -mclflushopt -mclwb -fmax-errors=5 -O2 $(FLUSH_FLAG) -m64 -mlzcnt -mpopcnt
CFLAGS_DEBUG :=-Iinclude -Iindex -mclflushopt -mclwb -fmax-errors=5 -g $(FLUSH_FLAG) -m64 -mlzcnt -mpopcnt
STD := -std=c++14

LINK_LIB:=-lpmemobj -lpthread -fopenmp
INDEX:=index/*.h

all: main datagen 
	@echo "finish make"

main: main.cc $(INDEX)
	$(CPP) $(CFLAGS) main.cc -o main $(LINK_LIB) $(STD)



datagen: gen.cc
	$(CPP) $(CFLAGS) gen.cc -o datagen $(STD)



debug: 
	$(CPP) $(CFLAGS_DEBUG) main.cc -o main $(LINK_LIB)
	$(CPP) $(CFLAGS_DEBUG) gen.cc -o datagen

.PHONY: clean
clean:
	@rm  main datagen