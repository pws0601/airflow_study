FROM apache/airflow:2.3.3
USER root
RUN mkdir -p /var/lib/apt/lists/partial && chmod 755 /var/lib/apt/lists/partial
RUN apt-get update && apt-get install -y gcc locales vim procps net-tools

USER airflow
RUN /usr/local/bin/pip install --retries 10 --timeout 50 --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org --upgrade pip
RUN /usr/local/bin/pip --retries 10 --timeout 50 --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org install setuptools
RUN /usr/local/bin/pip --retries 10 --timeout 50 --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org install wheel
RUN /usr/local/bin/pip --retries 10 --timeout 50 --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org install pymongo==4.3.3 kiwipiepy==0.15.1 aiohttp==3.8.1
USER default
