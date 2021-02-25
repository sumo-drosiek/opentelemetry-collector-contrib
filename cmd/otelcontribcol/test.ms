    1  cd /systemd
    2  ./configure 
    3  apk add meson
    4  ./configure 
    5  apk add m4
    6  ./configure 
    7  apk add coreutils
    8  ./configure 
    9  apk add gperf
   10  ./configure 
   16  apk add libcap-dev

   echo 'prefix=/usr
exec_prefix=/usr
libdir=/usr/lib
includedir=/usr/include

Name: mount
Description: mount library
Version: 2.29.1
Requires.private: blkid
Cflags: -I${includedir}/libmount
Libs: -L${libdir} -lmount' > /usr/lib/pkgconfig/mount.pc
apk add blkid