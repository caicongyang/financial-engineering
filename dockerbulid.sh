
#!/bin/bash

# 构建镜像
docker build -t financial-data-processor:v1 .

# 打标签
docker tag financial-data-processor:v1 caicongyang/financial-data-processor:v1

# 推送镜像
docker push caicongyang/financial-data-processor:v1


