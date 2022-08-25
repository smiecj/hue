from setuptools import setup, find_packages

packages = find_packages()
print("[test] packages: {}".format(packages))

if __name__ == '__main__':
    
    setup(
        name="ranger_client",
        version="1.0",
        author="smiecj",
        description="ranger api client",
        url="https://github.com/smiecj/ranger-client", 
        packages=find_packages(),
        install_requires=['beautifulsoup4==4.9.3', 'requests==2.18.4'],
        entry_points = { 'desktop.sdk.lib': 'ranger_client=ranger_client' }
    )