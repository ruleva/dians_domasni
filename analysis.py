import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # This forces the use of a non-interactive backend
import matplotlib.pyplot as plt

import seaborn as sns
import ta.momentum
import os


def parse_numeric_columns(data, columns):
    for column in columns:
        if column in data.columns:
            data[column] = data[column].astype(str)  # Ensure all values are strings
            data[column] = data[column].str.replace('.', '', regex=False)  # Remove thousands separator
            data[column] = data[column].str.replace(',', '.', regex=False)  # Replace comma with dot
            data[column] = pd.to_numeric(data[column], errors='coerce')  # Convert to float, handling errors
    return data

def technical_analysis(data):
    mas_and_osci = {}
    # Moving Averages (MA)
    mas_and_osci['SMA_50'] = data['Цена на последна трансакција'].rolling(window=50).mean()
    mas_and_osci['SMA_200'] = data['Цена на последна трансакција'].rolling(window=200).mean()
    mas_and_osci['EMA_50'] = data['Цена на последна трансакција'].ewm(span=50, adjust=False).mean()
    mas_and_osci['EMA_200'] = data['Цена на последна трансакција'].ewm(span=200, adjust=False).mean()
    mas_and_osci['WMA_50'] = data['Цена на последна трансакција'].rolling(window=50).apply(
        lambda x: (x * range(1, len(x) + 1)).sum() / sum(range(1, len(x) + 1)))  # Weighted Moving Average

    # Oscillators
    mas_and_osci['RSI'] = ta.momentum.RSIIndicator(data['Цена на последна трансакција'], window=14).rsi()
    mas_and_osci['Stochastic'] = ta.momentum.StochasticOscillator(data['Макс.'], data['Мин.'],
                                                                  data['Цена на последна трансакција'],
                                                                  window=14).stoch()
    mas_and_osci['Williams_R'] = ta.momentum.WilliamsRIndicator(data['Макс.'], data['Мин.'],
                                                                data['Цена на последна трансакција'],
                                                                lbp=14).williams_r()
    mas_and_osci['Awesome_Oscillator'] = ta.momentum.AwesomeOscillatorIndicator(data['Макс.'],
                                                                                data['Мин.']).awesome_oscillator()
    mas_and_osci['CCI'] = (data['Цена на последна трансакција'] - data['Цена на последна трансакција'].rolling(
        window=20).mean()) / (
                                  0.015 * data['Цена на последна трансакција'].rolling(window=20).std())
    return pd.DataFrame(mas_and_osci, index=data.index)


def generate_signals(indicators):
    signals = pd.DataFrame(index=indicators.index)
    signals['Buy'] = (indicators['RSI'] < 30) & (indicators['SMA_50'] > indicators['SMA_200'])
    signals['Sell'] = (indicators['RSI'] > 70) & (indicators['SMA_50'] < indicators['SMA_200'])
    signals['Hold'] = ~signals['Buy'] & ~signals['Sell']
    return signals


def resample_data(data, timeframe):
    """Resample data for different timeframes."""
    if timeframe == 'weekly':
        data = data.resample('W').agg({
            'Цена на последна трансакција': 'last',
            'Макс.': 'max',
            'Мин.': 'min'
        })
    elif timeframe == 'monthly':
        data = data.resample('M').agg({
            'Цена на последна трансакција': 'last',
            'Макс.': 'max',
            'Мин.': 'min'
        })
    return data


def process_csv_files(directory):
    """Processes all issuer CSV files in the given directory."""
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            filepath = os.path.join(directory, file)
            print(f"Processing file: {file}")

            # Load data
            try:
                data = pd.read_csv(filepath, parse_dates=['Датум'], index_col='Датум', dayfirst=True)

                # Check if data is empty
                if data.empty:
                    print(f"Warning: The file {file} is empty. Skipping...")
                    continue

                # Parse numeric columns
                numeric_columns = ['Цена на последна трансакција', 'Макс.', 'Мин.', 'Количина',
                                   'Промет во БЕСТ во денари',
                                   'Вкупен промет во денари']
                data = parse_numeric_columns(data, numeric_columns)

                # Handle missing or invalid data
                if data['Цена на последна трансакција'].isnull().any():
                    print(f"Warning: Missing or invalid data in 'Цена на последна трансакција' for file {file}")
                    data = data.dropna(subset=['Цена на последна трансакција'])

            except pd.errors.EmptyDataError:
                print(f"Error: {file} is empty or contains no data. Skipping...")
                continue  # Skip the file if it's empty or malformed

            except Exception as e:
                print(f"An error occurred with {file}: {e}")
                continue  # Skip the file if any error occurs
            for timeframe in ['daily', 'weekly', 'monthly']:
                    timeframe_data = data.copy()
                    if timeframe != 'daily':
                        timeframe_data = resample_data(data, timeframe)

                    indicators = technical_analysis(timeframe_data)
                    signals = generate_signals(indicators)

                    print(f"Issuer: {file.split('.')[0]}, Timeframe: {timeframe}")
                    print(signals.tail())

                    # Visualization
                    plt.figure(figsize=(10, 6))
                    plt.plot(timeframe_data['Цена на последна трансакција'], label='Price', color='blue')
                    plt.plot(indicators['SMA_50'], label='SMA_50', color='orange')
                    plt.plot(indicators['SMA_200'], label='SMA_200', color='red')
                    plt.scatter(signals.index[signals['Buy']],
                                timeframe_data['Цена на последна трансакција'][signals['Buy']],
                                marker='^', color='green', label='Buy Signal')
                    plt.scatter(signals.index[signals['Sell']],
                                timeframe_data['Цена на последна трансакција'][signals['Sell']],
                                marker='v', color='red', label='Sell Signal')
                    plt.title(f"{file.split('.')[0]} - {timeframe.capitalize()} Analysis")
                    plt.legend()
                    plt.show()


# Specify the directory where scraped CSV files are stored
directory = 'issuers'  # Change this to the folder containing the scraped files
process_csv_files(directory)
