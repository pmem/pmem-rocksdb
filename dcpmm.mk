ROCKSDB_ON_DCPMM := 1

ifdef ROCKSDB_ON_DCPMM

ifdef PMDK_LIBRARY_PATH
LDFLAGS += -L$(PMDK_LIBRARY_PATH) -Wl,-rpath=$(PMDK_LIBRARY_PATH)
else
LDFLAGS += -L/usr/local/lib/ -L/usr/local/lib64/
endif
LDFLAGS += -lpmem -lpmemobj

ifdef PMDK_INCLUDE_PATH
CXXFLAGS += -I$(PMDK_INCLUDE_PATH)
else
CXXFLAGS += -I/usr/local/include
endif
CXXFLAGS += -DON_DCPMM

endif