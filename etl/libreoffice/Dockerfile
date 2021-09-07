FROM openjdk:8-jre-slim

RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get -y install \
  libreoffice-common \ 
  unoconv \
  hyphen-af hyphen-en-us \
  fonts-dejavu fonts-dejavu-core fonts-dejavu-extra \
  fonts-droid-fallback fonts-dustin fonts-f500 fonts-fanwood fonts-freefont-ttf fonts-liberation \
  fonts-lmodern fonts-lyx fonts-sil-gentium fonts-texgyre fonts-tlwg-purisa fonts-opensymbol && \
  rm -rf /var/lib/apt/lists/*

EXPOSE 8100

CMD /usr/bin/libreoffice --headless --invisible --nocrashreport --nodefault --nofirststartwizard --nologo --norestore --accept="socket,host=0.0.0.0,port=8100,tcpNoDelay=1;urp;StarOffice.ComponentContext"
 