{
  "targets": [
    {
      "target_name": "node-masstree",
      "sources": [
		"node-masstree.cc"
	 ], 
	"cflags": [
	  "-std=c++11",
	],
	"//": ["config.h", "misc.cc", "testrunner.cc", "kvio.cc",
        "json.cc", "string.cc", "straccum.cc", "str.cc", "msgpack.cc",
            "clp.cc", "kvrandom.cc", "compiler.cc", "kvthread.c"]

    }
  ]
}

