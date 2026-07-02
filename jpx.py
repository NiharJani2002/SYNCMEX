# Nihar Mahesh Jani — niharmaheshjani@gmail.com
#
# JPX downloader. Reads the ticker list from config.yaml and downloads
# each stock's daily OHLCV history via yfinance, saving directly to
# stock_exchange/JPX/{ticker}.csv
#
# Run standalone if you only need this exchange:
#   python3 jpx.py

from base import main

if __name__ == "__main__":
    main("JPX")
