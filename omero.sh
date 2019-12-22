podman pod create --name omero -p 4064:4064
podman run -d --name omerodb --pod omero -e POSTGRES_PASSWORD=omero postgres:10
podman run -d --name omeroserver --pod omero -e CONFIG_omero_db_host=localhost -e CONFIG_omero_db_user=postgres -e CONFIG_omero_db_name=postgres openmicroscopy/omero-server:5.6
