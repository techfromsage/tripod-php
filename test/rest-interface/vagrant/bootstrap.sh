apt-get update -y
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | tee /etc/apt/sources.list.d/mongodb.list
apt-get update -y
apt-get install -y mongodb-10gen=2.2.2 -y
apt-get install php5 php-pear -y
yes '' | pecl install mongo
apt-get install git siege -y
a2enmod rewrite
touch /etc/php5/apache2/conf.d/mongo.ini
echo "extension=mongo.so" > /etc/php5/apache2/conf.d/mongo.ini
sed -i "s/DocumentRoot \/var\/www/DocumentRoot \/vagrant/" /etc/apache2/sites-enabled/000-default
sed -i "s/Directory \/var\/www\//Directory \/vagrant\//" /etc/apache2/sites-enabled/000-default
sed -i "s/AllowOverride None/AllowOverride All/" /etc/apache2/sites-enabled/000-default
service apache2 restart
