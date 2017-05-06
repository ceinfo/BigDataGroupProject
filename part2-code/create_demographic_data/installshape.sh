#!/bin/bash
# shapely installation
wget http://download.osgeo.org/geos/geos-3.5.0.tar.bz2
tar jxf geos-3.5.0.tar.bz2
cd geos-3.5.0 && ./configure --prefix=$HOME/geos-bin && make && make install
sudo cp /home/hadoop/geos-bin/lib/* /usr/lib
sudo /bin/sh -c 'echo "/usr/lib" >> /etc/ld.so.conf'
sudo /bin/sh -c 'echo "/usr/lib/local" >> /etc/ld.so.conf'
sudo /sbin/ldconfig
sudo /bin/sh -c 'echo -e "\nexport LD_LIBRARY_PATH=/usr/lib" >> /home/hadoop/.bashrc'
source /home/hadoop/.bashrc
sudo pip install shapely
echo "Shapely installation complete"
pip install https://pypi.python.org/packages/74/84/fa80c5e92854c7456b591f6e797c5be18315994afd3ef16a58694e1b5eb1/Geohash-1.0.tar.gz
#
exit 0

#Credit:  http://stackoverflow.com/questions/36217090/how-to-get-python-libraries-in-pyspark
