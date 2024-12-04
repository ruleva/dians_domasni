from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash
import filteri  # Your scraping function
import threading  # To handle scraping in the background
import time  # For simulating delays during scraping

app = Flask(__name__)
app.secret_key = 'aasecretkey'

users = {'Anastasija': 'aa', 'Iva': 'ii'}
scraping_progress = {
    "progress": 0,
    "log": [],
    "current_issuer": ""
}

@app.route('/')
def index():
    if 'username' in session:
        return redirect(url_for('welcome'))
    return render_template('login.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if username in users and users[username] == password:
            session['username'] = username
            flash("Login successful!", "success")
            return redirect(url_for('welcome'))
        else:
            flash("Invalid username or password. Please try again.", "error")
            return render_template('login.html')
    return render_template('login.html')


@app.route('/welcome')
def welcome():
    if 'username' in session:
        return render_template('welcome.html', username=session['username'])
    else:
        flash("You must be logged in to access the welcome page.", "error")
        return redirect(url_for('login'))


@app.route('/scraping')
def scraping():
    return render_template('scraping.html')


@app.route('/start_scraping', methods=['POST'])
def start_scraping():
    # Clear any previous progress
    global scraping_progress
    scraping_progress = {
        "progress": 0,
        "log": ["Starting scraping process..."],
        "current_issuer": ""
    }
    # Start scraping in a separate thread to avoid blocking the main thread
    threading.Thread(target=run_scraping_process).start()
    return redirect(url_for('scraping'))


def run_scraping_process():
    def update_progress(progress=None, message=None):
        if progress is not None:
            scraping_progress["progress"] = progress
        if message:
            scraping_progress["log"].append(message)

    try:
        # Call the `pipe()` function with a callback
        filteri.pipe(progress_callback=update_progress)
        scraping_progress["log"].append("Scraping process successfully completed.")
    except Exception as e:
        scraping_progress["log"].append(f"Error during scraping: {str(e)}")


@app.route('/get_scraping_progress', methods=['GET'])
def get_scraping_progress():
    return jsonify(scraping_progress)
@app.route('/logout')
def logout():
    session.clear()  # Clears all session data
    flash("You have been logged out successfully.", "info")
    return redirect(url_for('login'))


if __name__ == '__main__':
    app.run(debug=True)
