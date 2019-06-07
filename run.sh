rm -rf out.log
PYTHONPATH="$PYTHONPATH:../bcdbr" python crawler.py 2>&1 | tee -a out.log
