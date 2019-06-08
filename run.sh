rm -rf out.log
rm -rf balances.sqlite
PYTHONPATH="$PYTHONPATH:../bcdbr" python crawler.py
