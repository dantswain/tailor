'''
This is a simple viewer for the output of the code classifier.

It reads the output json file and displays the results on a simple web page.
Run `python tailor/view_predictions.py` to launch it.
'''
import json

from flask import Flask, render_template

app = Flask(__name__)


def load_predictions():
    results = {}

    with open("language_predictions.json") as f:
        data = json.load(f)

        for datum in data:
            language = datum['language']
            code = datum['value'].strip()

            if language not in results:
                results[language] = []

            results[language].append(code)

    return results


@app.route("/")
def template_test():
    predictions = load_predictions()
    # put text last
    text = predictions.pop('text')

    # a list of tuples is easier to deal with in jinja
    predictions = sorted(predictions.items(), key=lambda s: -len(s[1]))
    predictions.append(('text', text))

    return render_template('predictions_view.html', predictions=predictions)


if __name__ == '__main__':
    app.run(debug=True)
