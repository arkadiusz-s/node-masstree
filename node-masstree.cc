/*
#include <stdlib.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/wait.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <math.h>
#include <fcntl.h>
*/
#include <stdio.h>
#include <unistd.h>

#include "masstree-beta/config.h"
#include "masstree-beta/json.hh"
using namespace lcdf;
#include "masstree-beta/kvio.hh"
#include "masstree-beta/mtclient.hh"
#include <node.h>
#include <v8.h>
#include "masstree-beta/mtclient.cc"
#include "masstree-beta/misc.cc"
#include "masstree-beta/testrunner.cc"
#include "masstree-beta/json.cc"
#include "masstree-beta/string.cc"
#include "masstree-beta/kvio.cc"
#include "masstree-beta/msgpack.cc"
#include "masstree-beta/straccum.cc"

/*
#include "kvstats.hh"
#include "kvtest.hh"
*/

//#include "kvrandom.hh"
//#include "clp.h"

using namespace v8;

KVConn *conn;
int seq = 0;


Handle<Value> convjson(Json inp) {
  if(inp.is_null()) {
    return v8::Null();
  }
  if(inp.is_number()) {
    if(inp.is_int()) {
      return Number::New(inp.to_i());
    }
    if(inp.is_unsigned()) {
      return Number::New(inp.to_u());
    }
    if(inp.is_double()) {
      return Number::New(inp.to_d());
    }
  }
  if(inp.is_bool()) {
    return Number::New(inp.to_b());
  }
  if(inp.is_string()) {
    return v8::String::New(inp.as_s().c_str());
  }
  if(inp.is_array()) {
    int len = inp.size();
    Json* dats = inp.array_data();
    Handle<Array> array = Array::New(len);
    for(int i=0;i<len;i++) {
      Json dat = dats[i];
      Handle<Value> v = convjson(dat);
      array->Set(i, v);
    }
    return array;
  }
  if(inp.is_object()) {
    printf("object\n");
    return v8::Null();
  }
  if(inp.is_primitive()) {
    printf("primitive\n");
    return v8::Null();
  }  
}

Handle<Value> sendGetWhole(const Arguments& args) {
  HandleScope scope;
  if(args.Length() < 1) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong number of arguments")));
    return scope.Close(Undefined());
  }
  if (!args[0]->IsString()) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong arguments")));
    return scope.Close(Undefined());
  }        
  v8::String::Utf8Value str(args[0]->ToString());
  char* c_arg = *str;
  conn->sendgetwhole(Str(c_arg), seq++);
  return scope.Close(Number::New(seq));
}

Handle<Value> getWhole(const Arguments& args) {
  HandleScope scope;
  sendGetWhole(args);
  conn->flush();
  const Json& result = conn->receive();
  return scope.Close(convjson(result));
}

Handle<Value> getWholeJSON(const Arguments& args) {
  HandleScope scope;
  sendGetWhole(args);
  conn->flush();
  const Json& result = conn->receive();
  lcdf::String s = result.unparse();  
  return scope.Close(v8::String::New(s.c_str()));    
}

Handle<Value> sendGetCol(const Arguments& args) {
  HandleScope scope;
  if(args.Length() < 2) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong number of arguments")));
    return scope.Close(Undefined());
  }
  if (!args[0]->IsString() || !args[1]->IsNumber()) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong arguments")));
    return scope.Close(Undefined());
  }
  v8::Local<v8::Int32> intValuecol = args[1]->ToInt32();
  if (intValuecol.IsEmpty()) {
      ThrowException(Exception::TypeError(v8::String::New("Bad column integer")));
      return scope.Close(Undefined());
  }
  int col = intValuecol->Value();

  v8::String::Utf8Value str(args[0]->ToString());
  char* c_arg = *str;
  conn->sendgetcol(c_arg, col, seq++);

  return scope.Close(Number::New(seq));
}

Handle<Value> getCol(const Arguments& args) {
  HandleScope scope;
  sendGetCol(args);
  conn->flush();
  const Json& result = conn->receive();
  return scope.Close(convjson(result));
}

Handle<Value> getColJSON(const Arguments& args) {
  HandleScope scope;
  sendGetCol(args);
  conn->flush();
  const Json& result = conn->receive();
  lcdf::String s = result.unparse();  
  return scope.Close(v8::String::New(s.c_str()));  
}

Handle<Value> sendGet(const Arguments& args) {
  HandleScope scope;
  if(args.Length() < 2) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong number of arguments")));
    return scope.Close(Undefined());
  }

  v8::String::Utf8Value str(args[0]->ToString());
  char* c_arg = *str;

  v8::Local<v8::Object> f = args[1]->ToObject(); //v8::Array::New(foobarCount);

  v8::Array *a = v8::Array::Cast(*args[1]);
  
  std::vector<unsigned> ff;
  int len = a->Length();

  for(unsigned int i = 0; i < len; i++){
    ff.push_back(f->Get(i)->ToInt32()->Value()); //TODO
  }
  
  conn->sendget(c_arg, ff, seq++);
  return scope.Close(Number::New(seq));
}

Handle<Value> get(const Arguments& args) {
  HandleScope scope;
  sendGet(args);
  conn->flush();
  const Json& result = conn->receive();
  return scope.Close(convjson(result));    
}

Handle<Value> getJSON(const Arguments& args) {
  HandleScope scope;
  sendGet(args);
  conn->flush();
  const Json& result = conn->receive();
  lcdf::String s = result.unparse();  
  return scope.Close(v8::String::New(s.c_str()));    
}


Handle<Value> sendPutCol(const Arguments& args) {
  HandleScope scope;
  if(args.Length() < 3) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong number of arguments")));
    return scope.Close(Undefined());
  }
  if (!args[0]->IsString() || !args[1]->IsNumber() || !args[2]->IsString()) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong arguments")));
    return scope.Close(Undefined());
  }
  v8::String::Utf8Value str(args[0]->ToString());
  char* c_arg = *str;

  v8::Local<v8::Int32> intValuecol = args[1]->ToInt32();
  if (intValuecol.IsEmpty()) {
      ThrowException(Exception::TypeError(v8::String::New("Bad column integer")));
      return scope.Close(Undefined());
  }
  int col = intValuecol->Value();

  v8::String::Utf8Value str2(args[2]->ToString());
  char* c_arg2 = *str2;
  
  conn->sendputcol(c_arg, col, c_arg2, seq++);
  return scope.Close(Number::New(seq));
}

Handle<Value> putCol(const Arguments& args) {
  HandleScope scope;
  sendPutCol(args);
  conn->flush();
  const Json& result = conn->receive();
  lcdf::String s = result.unparse();
  return scope.Close(v8::String::New(s.c_str()));  
}

Handle<Value> putColJSON(const Arguments& args) {
  HandleScope scope;
  sendPutCol(args);
  conn->flush();
  const Json& result = conn->receive();
  return scope.Close(convjson(result));
}

Handle<Value> sendPutWhole(const Arguments& args) {
  HandleScope scope;
  if(args.Length() < 2) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong number of arguments")));
    return scope.Close(Undefined());
  }
  if (!args[0]->IsString() || !args[1]->IsString()) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong arguments")));
    return scope.Close(Undefined());
  }
        
  v8::String::Utf8Value str(args[0]->ToString());
  char* c_arg = *str;

  v8::String::Utf8Value str2(args[1]->ToString());
  char* c_arg2 = *str2;
  
  conn->sendputwhole(Str(c_arg), Str(c_arg2), seq++);

  return scope.Close(Number::New(seq));
}

Handle<Value> putWhole(const Arguments& args) {
  HandleScope scope;
  sendPutWhole(args);
  conn->flush();
  const Json& result = conn->receive();
  return scope.Close(convjson(result));
}

Handle<Value> putWholeJSON(const Arguments& args) {
  HandleScope scope;
  sendPutWhole(args);
  conn->flush();
  const Json& result = conn->receive();
  lcdf::String s = result.unparse();  
  return scope.Close(v8::String::New(s.c_str()));  
}

Handle<Value> sendRemove(const Arguments& args) {
  HandleScope scope;
  if(args.Length() < 1) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong number of arguments")));
    return scope.Close(Undefined());
  }
  
  if (!args[0]->IsString()) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong arguments")));
    return scope.Close(Undefined());
  }
        
  v8::String::Utf8Value str(args[0]->ToString());
  char* c_arg = *str;

  conn->sendremove(c_arg, seq++);
  return scope.Close(Number::New(seq));
}  

Handle<Value> remove(const Arguments& args) {
  HandleScope scope;
  sendRemove(args);
  conn->flush();
  const Json& result = conn->receive();
  return scope.Close(convjson(result));
}

Handle<Value> removeJSON(const Arguments& args) {
  HandleScope scope;
  sendRemove(args);
  conn->flush();
  const Json& result = conn->receive();
  lcdf::String s = result.unparse();  
  return scope.Close(v8::String::New(s.c_str()));    
}

Handle<Value> sendScanWhole(const Arguments& args) {
  HandleScope scope;
  if(args.Length() < 2) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong number of arguments")));
    return scope.Close(Undefined());
  }

  if (!args[0]->IsString() || !args[1]->IsNumber()) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong arguments")));
    return scope.Close(Undefined());
  }

  v8::Local<v8::Int32> intValuec = args[1]->ToInt32();
  if (intValuec.IsEmpty()) {
      ThrowException(Exception::TypeError(v8::String::New("Bad count integer")));
      return scope.Close(Undefined());
  }
  int count = intValuec->Value();
        
  v8::String::Utf8Value str(args[0]->ToString());
  char* c_arg = *str;

  conn->sendscanwhole(c_arg, count, seq++);
  
  return scope.Close(Number::New(seq));
}

Handle<Value> scanWhole(const Arguments& args) {
  HandleScope scope;
  sendScanWhole(args);
  conn->flush();
  const Json& result = conn->receive();
  return scope.Close(convjson(result));    
}

Handle<Value> scanWholeJSON(const Arguments& args) {
  HandleScope scope;
  sendScanWhole(args);
  conn->flush();
  const Json& result = conn->receive();
  lcdf::String s = result.unparse();  
  return scope.Close(v8::String::New(s.c_str()));    
}

Handle<Value> sendScan(const Arguments& args) {
  HandleScope scope;
  if(args.Length() < 3) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong number of arguments")));
    return scope.Close(Undefined());
  }

  if (!args[0]->IsString() || !args[1]->IsArray() || !args[2]->IsNumber()) {
    ThrowException(Exception::TypeError(v8::String::New("Wrong arguments")));
    return scope.Close(Undefined());
  }


  v8::String::Utf8Value str(args[0]->ToString());
  char* c_arg = *str;

  v8::Local<v8::Int32> intValuec = args[2]->ToInt32();
  if (intValuec.IsEmpty()) {
      ThrowException(Exception::TypeError(v8::String::New("Bad count integer")));
      return scope.Close(Undefined());
  }
  int count = intValuec->Value();

  v8::Local<v8::Object> f = args[1]->ToObject(); //v8::Array::New(foobarCount);

  v8::Array *a = v8::Array::Cast(*args[1]);
  
  std::vector<unsigned> ff;
  int len = a->Length();

  for(unsigned int i = 0; i < len; i++){
    ff.push_back(f->Get(i)->ToInt32()->Value()); //TODO
  }
  
  conn->sendscan(c_arg, ff, count, seq++);
  return scope.Close(Number::New(seq));
  
}

Handle<Value> scan(const Arguments& args) {
  HandleScope scope;
  sendScan(args);
  conn->flush();
  const Json& result = conn->receive();
  return scope.Close(convjson(result));      
}

Handle<Value> scanJSON(const Arguments& args) {
  HandleScope scope;
  sendScan(args);
  conn->flush();
  const Json& result = conn->receive();
  lcdf::String s = result.unparse();  
  return scope.Close(v8::String::New(s.c_str()));      
}

Handle<Value> checkpoint(const Arguments& args) {
  HandleScope scope;
  
  conn->checkpoint(0); //TODO: doublecheck receive

  return scope.Close(Boolean::New(true));
}

Handle<Value> flush(const Arguments& args) {
  HandleScope scope;
  
  conn->flush();

  return scope.Close(Boolean::New(true));
}

Handle<Value> check(const Arguments& args) {
  HandleScope scope;
  int tryhard = 0;
  if(args.Length() > 0) {
      v8::Local<v8::Int32> intValue = args[0]->ToInt32();
      if (intValue.IsEmpty()) {
          ThrowException(Exception::TypeError(v8::String::New("Bad tryhard integer")));
          return scope.Close(Undefined());
      }
      tryhard = intValue->Value();
  }
  int r = conn->check(tryhard);
  return scope.Close(Number::New(r));
}

Json _receive(const Arguments& args) {
    const Json& result = conn->receive();
    return result;
}

Handle<Value> receive(const Arguments& args) {
  const Json& result = conn->receive();
  return convjson(result);
}

Handle<Value> receiveJSON(const Arguments& args) {
  HandleScope scope;
  const Json& result = conn->receive();
  lcdf::String s = result.unparse();
  return scope.Close(v8::String::New(s.c_str()));
}


void init(Handle<Object> exports) {

  conn = new KVConn("localhost", 2117);

  exports->Set(v8::String::NewSymbol("sendGetWhole"), FunctionTemplate::New(sendGetWhole)->GetFunction());
  exports->Set(v8::String::NewSymbol("getWhole"), FunctionTemplate::New(getWhole)->GetFunction());
  exports->Set(v8::String::NewSymbol("getWholeJSON"), FunctionTemplate::New(getWholeJSON)->GetFunction());
  exports->Set(v8::String::NewSymbol("sendGetCol"), FunctionTemplate::New(sendGetCol)->GetFunction());
  exports->Set(v8::String::NewSymbol("getCol"), FunctionTemplate::New(getCol)->GetFunction());
  exports->Set(v8::String::NewSymbol("getColJSON"), FunctionTemplate::New(getColJSON)->GetFunction());
  exports->Set(v8::String::NewSymbol("sendGet"), FunctionTemplate::New(sendGet)->GetFunction());
  exports->Set(v8::String::NewSymbol("get"), FunctionTemplate::New(get)->GetFunction());
  exports->Set(v8::String::NewSymbol("getJSON"), FunctionTemplate::New(getJSON)->GetFunction());
  
  exports->Set(v8::String::NewSymbol("sendPutCol"), FunctionTemplate::New(sendPutCol)->GetFunction());  
  exports->Set(v8::String::NewSymbol("putCol"), FunctionTemplate::New(putCol)->GetFunction());  
  exports->Set(v8::String::NewSymbol("putColJSON"), FunctionTemplate::New(putColJSON)->GetFunction());  
  exports->Set(v8::String::NewSymbol("sendPutWhole"), FunctionTemplate::New(sendPutWhole)->GetFunction());
  exports->Set(v8::String::NewSymbol("putWhole"), FunctionTemplate::New(putWhole)->GetFunction());  
  exports->Set(v8::String::NewSymbol("putWholeJSON"), FunctionTemplate::New(putWholeJSON)->GetFunction());  
  
  exports->Set(v8::String::NewSymbol("sendRemove"), FunctionTemplate::New(sendRemove)->GetFunction());
  exports->Set(v8::String::NewSymbol("remove"), FunctionTemplate::New(remove)->GetFunction());
  exports->Set(v8::String::NewSymbol("removeJSON"), FunctionTemplate::New(removeJSON)->GetFunction());
  
  exports->Set(v8::String::NewSymbol("sendScanWhole"), FunctionTemplate::New(sendScanWhole)->GetFunction());
  exports->Set(v8::String::NewSymbol("scanWhole"), FunctionTemplate::New(scanWhole)->GetFunction());
  exports->Set(v8::String::NewSymbol("scanWholeJSON"), FunctionTemplate::New(scanWholeJSON)->GetFunction());
  exports->Set(v8::String::NewSymbol("sendScan"), FunctionTemplate::New(sendScan)->GetFunction());
  exports->Set(v8::String::NewSymbol("scan"), FunctionTemplate::New(scan)->GetFunction());
  exports->Set(v8::String::NewSymbol("scanJSON"), FunctionTemplate::New(scanJSON)->GetFunction());
  

  exports->Set(v8::String::NewSymbol("checkpoint"), FunctionTemplate::New(checkpoint)->GetFunction());
  exports->Set(v8::String::NewSymbol("flush"), FunctionTemplate::New(flush)->GetFunction());
  exports->Set(v8::String::NewSymbol("check"), FunctionTemplate::New(check)->GetFunction());
  exports->Set(v8::String::NewSymbol("receive"), FunctionTemplate::New(receive)->GetFunction());
  exports->Set(v8::String::NewSymbol("receiveJSON"), FunctionTemplate::New(receiveJSON)->GetFunction());
  

}

NODE_MODULE(node_masstree, init)
