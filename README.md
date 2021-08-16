# telethon_grabber
a script to obtain group messages from telegram and store them in a dataframe for later analysis

## how to use
* create a venv `python3 -m venv .venv` and install requirements `pip install -r requirements.pop`
* have config.ini ready, see _config.ini.example_
* supply the script a lower date in YYYY-MM-DD format
* run it
```bash
python main.py 2021-03-15
```

## resources
* https://tl.telethon.dev/methods/index.html
* https://medium.com/better-programming/how-to-get-data-from-telegram-82af55268a4b
