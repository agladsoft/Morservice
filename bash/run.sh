while true;
do
	python3 ${XL_IDP_PATH_MORSERVICE}/Ref.py;
	python3 ${XL_IDP_PATH_MORSERVICE}/missing_data.py;
	sleep 30;
done
