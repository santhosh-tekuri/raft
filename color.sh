awk \
-v testCase="\033[44;39m" \
-v test="\033[35m" \
-v cnd="\033[31m" \
-v repl="\033[33m" \
-v flr="\033[32m" \
-v ldr="\033[1;37m" \
-v fsm="\033[36m" \
'
/\[testing\]----------------------- Test/ {print testCase $0 "\033[0m"; system(""); next}
/\[testing\]/ {print test $0 "\033[0m"; system(""); next}
/ R \| / {print repl $0 "\033[0m"; system(""); next}
/ L \| / {print ldr $0 "\033[0m"; system(""); next}
/ F \| / {print flr $0 "\033[0m"; system(""); next}
/ C \| / {print cnd $0 "\033[0m"; system(""); next}
/ FSM \| / {print fsm $0 "\033[0m"; system(""); next}
1; system("")
'