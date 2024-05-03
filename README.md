# BioMongoDB tools

BioMongoDB is envisioned as a MongoDB database to facilitate the storage of information derived from various initiatives within the BioTeam.

BioMongo tools is a set of tools designed to manage the BioMongo database. The tools provided here facilitate the manage of information (insertion of information, update of information, etc.) in the BioMongoDB and maintain a log of these changes within the database itself. This log will be named `meta`.

## Installation

To install BioMongo tools, follow these steps:


1. Clone this repository to your local machine:

```
git clone git@github.com:m-huertasp/ega-biomongo-tools.git
```

2. Install the required dependencies:

```
pip install -r requirements.txt
```


## Usage

BioMongo tools should be as easy to use as possible. The only thing you need to do is to modify the [conf.py](https://github.com/m-huertasp/ega-biomongo-tools/blob/main/conf.py) file taking into account your needs.

To understand how to use the tool, I have written some use cases explained [here](https://docs.google.com/document/d/1rVnTp6rVefees6J4kwp1Thaq4HapLysS452wy5mUuKM/edit?usp=sharing).