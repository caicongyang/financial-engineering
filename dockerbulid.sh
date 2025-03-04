
#!/bin/bash

# 构建镜像
docker build -t financial-data-processor:latest .

# 打标签
docker tag financial-data-processor:latest caicongyang/financial-data-processor:latest

# 推送镜像
docker push caicongyang/financial-data-processor:latest


