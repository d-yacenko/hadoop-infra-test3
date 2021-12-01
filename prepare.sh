./gradlew jar
docker build -t dyacenko/hadoop-infra-test3 .
docker login
docker push dyacenko/hadoop-infra-test3
#kubectl run -it --rm hadoop-evnp --image=dyacenko/hadoop-infra-test3
#kubectl exec dyacenko/hadoop-infra-test3 -- curl -s localhost:8080
