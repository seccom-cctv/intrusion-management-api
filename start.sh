sudo docker build -t intrusion-management-api .
sudo docker run -d -p 8080:8080 --name intrusion-management-api intrusion-management-api