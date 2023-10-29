Apache Airflow
======================

.. image:: https://d25lcipzij17d.cloudfront.net/badge.svg?id=py&r=r&ts=1683906897&type=6e&v=2.7.2&x2=0
    :target: https://pypi.org/project/apache-airflow/

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

.. image:: https://img.shields.io/:license-Apache%202-blue.svg
    :target: https://www.apache.org/licenses/LICENSE-2.0.txt

Getting Started
-----------------------

Install Airflow on Ubuntu

.. code-block:: console

    $ sudo apt-get install -y python3-pip python3-venv
    $ mkdir airflow
    $ cd airflow
    # create python enviroment
    $ python3 -m venv airflow-env
    # activate python enviroment
    $ source airflow-env/bin/activate
    $ pip install apache-airflow
    $ airflow db init
    $ airflow users create --role Admin --username tubt2 --email tubt2@vpbank.com.vn --firstname Tu --lastname Thanh --password Thanhtu110694*

Start airflow

.. code-block:: console

    $ sudo su -
    $ cd /home/tubt/airflow
    $ source airflow-env/bin/activate
    $ nohup airflow scheduler > scheduler.log 2>&1 &
    $ nohup airflow webserver -p 8080 > webserver.log 2>&1 & 