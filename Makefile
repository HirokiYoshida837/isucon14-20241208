
# all
.PHONY: all.restart
all.restart: mysql.restart nginx.restart isuride.restart


# nginx

.PHONY: nginx.restart
nginx.restart:
	sudo systemctl restart nginx

.PHONY: nginx.logs
nginx.logs:
	journalctl -f -u nginx

# mysql

.PHONY: mysql.restart
mysql.restart:
	sudo systemctl restart mysql

# .PHONY: mysql.logs
# mysql.logs: journalctl -f -u nginx


# isuridex

.PHONY: isuride.restart
isuride.restart:
	sudo systemctl restart isupipe-go.service

.PHONY: isuride.build
isuride.build:
	cd ./go ; go build -o isuride;

# ghコマンドでmainブランチ最新を取得し、build・restartを行います。
.PHONY: isuride.deploy
isuride.deploy: gh repo sync origin/main;
	git switch main;
	make isuride.build;
	make isuride.restart;
