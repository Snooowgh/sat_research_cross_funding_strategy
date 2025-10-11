#!/bin/bash
git add . && git commit -m "deploy" || echo "代码已提交"
#git pull --no-edit
#git push origin master
git push satresearch master
#git remote add satresearch root@45.76.205.243:/root/sat_research_cross_funding_strategy/.git/
