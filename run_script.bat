@echo off
call C:\Users\82102\anaconda3\Scripts\activate base
cd C:\Users\82102\OneDrive - SNU\문서\lab_Chasm\데이터 쌓기\data_수정
powercfg /waketimers > wake_log.txt
timeout /t 10
python main.py
schtasks /end /tn "LabChasm"
exit 0