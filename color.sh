awk \
-v test="\033[35m" \
-v cnd="\033[31m" \
-v repl="\033[33m" \
-v flr="\033[32m" \
-v ldr="\033[37m" \
'
/\[testing\]/ {print test $0 "\033[39m"; system(""); next}
/ L \| M/ {print repl $0 "\033[39m"; system(""); next}
/ L \| / {print ldr $0 "\033[39m"; system(""); next}
/ F \| / {print flr $0 "\033[39m"; system(""); next}
/ C \| / {print cnd $0 "\033[39m"; system(""); next}
1; system("")
'