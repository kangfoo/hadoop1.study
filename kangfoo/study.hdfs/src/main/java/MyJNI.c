#include "MyJNI.h"
#include <stdio.h>

JNIEXPORT void JNICALL Java_MyJNI_print
  (JNIEnv *env, jobject obj, jstring jstr){
	char *cstr =(char *)(*env)->GetStringUTFChars(env,jstr,NULL);
	printf("%s\n",cstr);

	(*env)->ReleaseStringUTFChars(env,jstr,cstr);
}
