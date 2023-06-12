# PostgreSql Docker 설치
docker pull postgres
docker run -d -p 10070:5432 -e POSTGRES_PASSWORD="airflow" --name PostgreSQL01 postgres:13

# 데이터 베이스를 통한 테스트용 쿼리
CREATE TABLE pageview_counts (
    pagename VARCHAR(50) NOT NULL,
    pageviewcount INT NOT NULL,
    datetime TIMESTAMP NOT NULL
)

INSERT INTO pageview_counts VALUES ('Google', 333, '2019-07-17T00:00:00');

# PostgresOperator 클래스를 가져오기 위한 패키지 설치
pip install apache-airflow-providers-postgres