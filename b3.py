# Nihar Mahesh Jani — niharmaheshjani@gmail.com
#
# B3 downloader. Reads the ticker list from config.yaml and downloads
# each stock's daily OHLCV history via yfinance, saving directly to
# stock_exchange/B3/{ticker}.csv
#
# Run standalone if you only need this exchange:
#   python3 b3.py

from base import main

if __name__ == "__main__":
    main("B3")
