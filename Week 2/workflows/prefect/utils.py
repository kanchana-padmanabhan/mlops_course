import os
import numpy as np
import yaml


def fourier_terms(value, period, num_terms):
    terms = []
    for i in range(1, num_terms + 1):
        terms.extend([np.sin(2 * np.pi * i * value / period),
                      np.cos(2 * np.pi * i * value / period)])
    return terms


def load_config(config_path):
    with open(os.path.join(config_path)) as file:
        config = yaml.safe_load(file)
    return config