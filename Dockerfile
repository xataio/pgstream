FROM scratch
COPY pgstream /usr/bin/pgstream
ENTRYPOINT [ "/usr/bin/pgstream" ]
