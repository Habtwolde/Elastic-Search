# ELSER Hybrid Search (Python + Elasticsearch)

This project provides a simple CLI to index a spreadsheet (CSV/XLSX) into Elasticsearch and run **hybrid semantic search** (ELSER + BM25) or **BM25-only**.

---


---

1. Modify the Docker-config.yml to
   
  ```powershell
services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:9.1.3
    container_name: es01
    environment:
      - discovery.type=single-node
      - node.roles=master,data,ingest,ml
      - xpack.security.enabled=true
      # Plain HTTP for dev/local use; keep TLS off since your notebook uses http://
      - xpack.security.http.ssl.enabled=false
      - xpack.security.transport.ssl.enabled=false
      # Trial allows ML/inference
      - xpack.license.self_generated.type=trial
      - xpack.ml.enabled=true
      # JVM memory (ELSER likes RAM). Raise if you can.
      - ES_JAVA_OPTS=-Xms4g -Xmx4g
      # Bootstrap the elastic superuser
      - ELASTIC_PASSWORD=changeme

      # 🔸 You usually DO NOT need a local model repo for the built-in ELSER system model.
      # 🔸 Leave this commented unless you are importing custom .zip models with the _import API.
      # - xpack.ml.model_repository=file:///usr/share/elasticsearch/config/models
    ulimits:
      memlock:
        soft: -1
        hard: -1
    ports:
      - "9200:9200"
      - "9300:9300"
    # If (and only if) you truly need a local model repo for custom models, then:
    # volumes:
    #   - type: bind
    #     source: "C:\\ml-models"         # Windows Docker Desktop bind; ensure file sharing is allowed
    #     target: /usr/share/elasticsearch/config/models
    #     read_only: true
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9200/"]
      interval: 10s
      timeout: 5s
      retries: 30

  # Optional: keep Kibana commented out if you aren’t allowed to use it
  # kibana:
  #   image: docker.elastic.co/kibana/kibana:9.1.3
  #   container_name: kb01
  #   depends_on:
  #     - elasticsearch
  #   environment:
  #     - ELASTICSEARCH_HOSTS=http://elasticsearch:9200
  #     # If you enable Kibana, you must set the kibana_system password inside ES first.
  #     # - ELASTICSEARCH_USERNAME=kibana_system
  #     # - ELASTICSEARCH_PASSWORD=...
  #     - SERVER_PUBLICBASEURL=http://localhost:5601
  #     - SERVER_HOST=0.0.0.0
  #     - XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  #   ports:
  #     - "5601:5601"

  # Optional: Logstash only if you truly need it
  # logstash:
  #   image: docker.elastic.co/logstash/logstash:9.1.3
  #   container_name: ls01
  #   depends_on:
  #     - elasticsearch
  #   environment:
  #     - LS_JAVA_OPTS=-Xms1g -Xmx1g
  #   volumes:
  #     - "C:\\Users\\dell\\oracle-to-es:/usr/share/logstash/pipeline"

```
2. Download/register the system model
PowerShell:
  ```powershell
$cred = New-Object pscredential('elastic',(ConvertTo-SecureString 'changeme' -AsPlainText -Force))
Invoke-RestMethod -Method POST -Authentication Basic -Credential $cred -Uri "http://localhost:9200/_ml/trained_models/.elser_model_2_linux-x86_64/_download"
```
Curl
  ```powershell
curl -u elastic:changeme -X POST "http://localhost:9200/_ml/trained_models/.elser_model_2_linux-x86_64/_download"
```
3. Start the deployment (allocation)
PowerShell:
  ```powershell
Invoke-RestMethod -Method PUT -Authentication Basic -Credential $cred -Uri "http://localhost:9200/_ml/trained_models/.elser_model_2_linux-x86_64/deployment/_start?timeout=5m"
```
Curl
  ```powershell
curl -u elastic:changeme -X PUT "http://localhost:9200/_ml/trained_models/.elser_model_2_linux-x86_64/deployment/_start?timeout=5m"
```
4. Verify allocation is running
PowerShell:
  ```powershell
Invoke-RestMethod -Authentication Basic -Credential $cred -Uri "http://localhost:9200/_ml/trained_models/.elser_model_2_linux-x86_64/_stats?timeout=30s"
```
Curl
  ```powershell
curl -u elastic:changeme "http://localhost:9200/_ml/trained_models/.elser_model_2_linux-x86_64/_stats?timeout=30s"
```
5. run the 'check_elastic.ipynb' to test the Elastic search verification works and the embedding model is found at the path.
6. Smoke-test inference (no ingest needed)
PowerShell:
  ```powershell
Invoke-RestMethod -Method POST -Authentication Basic -Credential $cred -Uri "http://localhost:9200/_ml/trained_models/.elser_model_2_linux-x86_64/_infer" -Body (@{
  docs = @(@{ text_field = "hello world" })
  inference_config = @{ text_expansion = @{} }
} | ConvertTo-Json -Depth 5) -ContentType "application/json"
```
7. Run the 
