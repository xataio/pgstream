FROM scratch
COPY pgstream /usr/local/bin/pgstream
ENTRYPOINT [ "/usr/local/bin/pgstream" ]
