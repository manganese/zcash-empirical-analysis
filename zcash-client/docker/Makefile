install:
	git clone https://github.com/zcash/zcash.git src && \
	cd src && \
	CURRENT=`git tag | grep -P '^v[\d\.]+$$' | sort --version-sort | tail -1` && \
	git checkout tags/$$CURRENT -b local-$$CURRENT && \
        /bin/bash ./zcutil/fetch-params.sh && \
        /bin/bash ./zcutil/build.sh -j4 && \
	cd src && \
        install -s -c zcashd zcash-cli -t /usr/local/bin 
	#./autogen.sh && \
	#./configure --disable-wallet --disable-zmq --disable-proton --disable-rust --with-libs=no && \
