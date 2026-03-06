FROM scratch
COPY mxruntime /mxruntime
COPY config.toml /etc/mx/config.toml
EXPOSE 8443 9090 9100
ENTRYPOINT ["/mxruntime", "--config", "/etc/mx/config.toml"]
