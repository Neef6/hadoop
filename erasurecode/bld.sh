rm -rf sample
gcc -Iinclude -g -ldl erasure_code.c erasure_coder.c erasure_coder_sample.c -o sample
if [ $? = 0 ]; then
  ./sample
fi
