# California Synthetic Population

Synthetic population for the state of California generated using
[PopulationSim](https://activitysim.github.io/populationsim/index.html).

This repository accompanies the following publication:

**Large Scale Integrated Simulation of Household Vehicle Fleet Composition with Geographically Explicit Synthetic Population**  
Naomi Panjaitan, Ling Jin, Caitlin Brown, Tin Ho, Anna Spurlock, Thomas Wenzel, Alina Lazar, Qianmiao Chen, Andrew J. Bae  
*2025 IEEE International Conference on Big Data (BigData)*  
Pages: 8306 to 8308  
DOI: https://doi.org/10.1109/BigData66926.2025.11402352

---

## Overview

This repository contains configuration files and workflow components used to generate a geographically explicit synthetic population for California using the PopulationSim framework.

Synthetic populations are widely used in transportation modeling, agent based simulation, and policy analysis because they reproduce key statistical characteristics of the real population while protecting individual privacy.

The synthetic population produced here supports large scale modeling of:

- transportation demand and activity based travel models
- vehicle ownership and fleet composition
- transportation energy use and electrification scenarios
- behavioral and policy simulations at statewide scale

The methodology integrates demographic marginal distributions and microdata sources to construct a statistically consistent representation of households and persons across California.

This synthetic population serves as the demographic foundation for modeling household vehicle fleet composition and adoption dynamics in the associated IEEE BigData publication.

---

## Associated Paper

The modeling framework and application are described in:

Panjaitan, N., Jin, L., Brown, C., Ho, T., Spurlock, A., Wenzel, T., Lazar, A., Chen, Q., and Bae, A. J. (2025).  
**Large Scale Integrated Simulation of Household Vehicle Fleet Composition with Geographically Explicit Synthetic Population.**  
Proceedings of the IEEE International Conference on Big Data (BigData).  
https://doi.org/10.1109/BigData66926.2025.11402352

---

## Internship Contribution

This work was conducted during **Andrew Bae's Summer 2023 research internship at Lawrence Berkeley National Laboratory (LBNL)** while he was an **undergraduate student at Stony Brook University**.

---

## Repository Structure

    California-Synthetic-Population/
    ├── populationsim/        PopulationSim configuration and synthesis pipeline
    ├── raw_data/             Input marginal distributions and source datasets
    ├── example_calm/         Example configuration for running the synthesis
    ├── sample/               Small example outputs
    └── README.md             Project documentation

---

## Data Access

The full synthesized California population dataset is not distributed directly through this repository due to its size.

To obtain:

- the full synthesized California population for 2017 to 2021
- instructions for reproducing the synthesis pipeline

please contact:

**Ling Jin**  
Lawrence Berkeley National Laboratory  
Email: LJin@lbl.gov

---

## Requirements

Population synthesis is implemented using:

- Python 3.8+
- PopulationSim
- ActivitySim ecosystem tools

The data-query and post-processing scripts in `sample/` additionally depend on
`pandas`, `numpy`, `census`, `requests`, `us`, `ruamel.yaml`, and `openpyxl`.
These are listed in [`requirements.txt`](requirements.txt) and can be installed
with:

    pip install -r requirements.txt

PopulationSim itself (and the ActivitySim packages it depends on) should be
installed following the upstream instructions, as it is not distributed on PyPI
in the same way:

https://activitysim.github.io/populationsim/

### Census API key

The Census-querying scripts read your Census API key from the `CENSUS_API_KEY`
environment variable (a free key is available at
https://api.census.gov/data/key_signup.html):

    export CENSUS_API_KEY=your_key_here

PopulationSim documentation:  
https://activitysim.github.io/populationsim/

---

## Reproducing the Synthetic Population

The repository contains configuration files and workflows needed to run the
synthesis pipeline. The end-to-end workflow is:

1. Prepare demographic marginal distributions and control totals
2. Configure PopulationSim synthesis settings
3. Run the population synthesis pipeline
4. Validate marginal consistency and demographic distributions
5. Export synthetic household and person records

The `sample/` directory contains the scripts that automate this workflow:

| Script | Purpose |
| --- | --- |
| `county_query.py` | Downloads ACS marginals and PUMS seed data from the Census API and builds the control totals, seed tables, and geographic crosswalk. |
| `download_data.py` | CLI wrapper that only downloads/prepares the input data for a state, year, and set of counties. |
| `query_and_synthesis.py` | CLI wrapper that prepares input data, updates the PopulationSim config, runs the synthesis, and collects outputs. |
| `run_populationsim.py` | Thin entry point that runs PopulationSim with the given config directories. |
| `generate_summary.py` | Aggregates the per-county summary files into validation statistics (MAPE, MdAPE, aggregate differences). |

### Example: run a single county

From inside the `sample/` directory:

    # Download input data only (state 06 = California, county 013 = Contra Costa)
    python download_data.py --state 06 --year 2017 --counties 013

    # Or download data, run synthesis, and summarize in one step
    python query_and_synthesis.py --state 06 --year 2017 --counties 013

The `--counties` argument accepts a single county FIPS code (e.g. `013`), the
preset `bay_area`, or the preset `all_CA` (all 58 California counties).

### Example configuration

The `example_calm/` directory contains a complete, ready-to-run PopulationSim
configuration (single-process under `configs/` and multiprocess under
`configs_mp/`) together with small sample input data under `data/`, so you can
exercise the pipeline without first downloading from the Census API.

---

## Citation

If you use this repository or the synthetic population generated by it in your research, please cite the following paper.

### BibTeX

    @inproceedings{panjaitan2025syntheticpopulation,
      title={Large Scale Integrated Simulation of Household Vehicle Fleet Composition with Geographically Explicit Synthetic Population},
      author={Panjaitan, Naomi and Jin, Ling and Brown, Caitlin and Ho, Tin and Spurlock, Anna and Wenzel, Thomas and Lazar, Alina and Chen, Qianmiao and Bae, Andrew J.},
      booktitle={Proceedings of the IEEE International Conference on Big Data},
      year={2025},
      pages={8306--8308},
      publisher={IEEE},
      doi={10.1109/BigData66926.2025.11402352}
    }

---

## License

This repository provides code and configuration for generating the synthetic population.

Usage of underlying datasets may be subject to the licensing terms of their respective providers. Please cite the associated publication when using this work.

---

## Acknowledgements

This work was conducted through collaboration between researchers at:

- Lawrence Berkeley National Laboratory
- Stony Brook University

and supports large scale modeling of transportation and vehicle fleet dynamics in California.
