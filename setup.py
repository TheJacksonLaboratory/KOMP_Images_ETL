from setuptools import setup

setup(name="DCCImagesPipeline",
      author="chentJAX",
      author_email="chent@jax.org",
      description="Application to download files from various sources(Omero, Climb etc) and upload them into JAX's "
                  "SFTP server",
      version='0.0.1',
      include_package_data=True,
      install_requires=[
          'requests',
          'mysql.connector',
          'paramiko',
          'importlib-metadata; python_version == "3.9"',
      ],
      # Create an entry point
      entry_points={
          'console_scripts': [
              'download_images = App:main',
          ],
      },
      )
