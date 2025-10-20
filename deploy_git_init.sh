#!/bin/bash
# 获取当前目录的绝对路径
current_dir=$(pwd)
# 目标文件路径
hook_file=".git/hooks/post-receive"
git config receive.denyCurrentBranch ignore
cat > "$hook_file" <<EOF
#!/bin/bash
unset GIT_DIR
cd "$current_dir"
git checkout -f
bash restart.sh
EOF
chmod +x "$hook_file"
echo "git remote add prod_server root@167.179.71.210:$current_dir/.git/"