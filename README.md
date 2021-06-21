
![Metaflow_Logo_Horizontal_FullColor_Ribbon_Dark_RGB](https://user-images.githubusercontent.com/763451/89453116-96a57e00-d713-11ea-9fa6-82b29d4d6eff.png)


# Metaflow

Metaflow is a human-friendly Python/R library that helps scientists and engineers build and manage real-life data science projects. Metaflow was originally developed at Netflix to boost productivity of data scientists who work on a wide variety of projects from classical statistics to state-of-the-art deep learning.

For more information, see [Metaflow's website](https://metaflow.org) and [documentation](https://docs.metaflow.org).

## Trying out Metaflow in Binder
Launch Sample Noteboook in Binder: [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/talebzeghmi/metaflow/tz/workshop?urlpath=lab/tree/workshop)

### What to expect
Once you launch a notebook environment in Binder, you wil see the following:

* `0-jupyter.ipynb`: Basics of Jupyter Notebook
* `1-basiscs.ipynb`: Composing and running a sample flow.
* `2-titanic.ipynb`: Running feature engineering, training and scoring on classic titanic dataset using Metaflow.
* `ui.ipynb`: A showcase of how UI for metaflow looks like. Inspect metadata, logs and more about your flows here.

Feel free to execute, modify and try out examples. Have fun!

### Support from Zillow AI Platform
* AI Platform Doc: http://analytics.pages.zgtools.net/artificial-intelligence/ai-platform/aip-docs/kubeflow/index.html
* Slack Channel: #ai-platform-user-community

## Getting Started

Getting up and running with Metaflow is easy. 

### Python
Install metaflow from [pypi](https://pypi.org/project/metaflow/):

```sh
pip install metaflow
```

and access tutorials by typing:

```sh
metaflow tutorials pull
```

### R

Install Metaflow from [github](https://github.com/Netflix/metaflow/tree/master/R):

```R
devtools::install_github("Netflix/metaflow", subdir="R")
metaflow::install()
```

and access tutorials by typing:

```R
metaflow::pull_tutorials()
```

## Get in Touch
There are several ways to get in touch with us:

* Open an issue at: https://github.com/Netflix/metaflow 
* Email us at: help@metaflow.org
* Chat with us on: http://chat.metaflow.org 

## Contributing
We welcome contributions to Metaflow. Please see our [contribution guide](https://docs.metaflow.org/introduction/contributing-to-metaflow) for more details.
