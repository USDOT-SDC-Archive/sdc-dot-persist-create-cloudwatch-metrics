import jinja2


class TemplateLoader:
    """ Wrapper for jinja2 """

    def __init__(self, templates_dir):
        """
        creates an instance of the jinja2.Environment class, pointing it to the directory that houses templates.
        :param templates_dir: str, directory with templates
        """
        self.template_loader = jinja2.Environment(loader=jinja2.FileSystemLoader(templates_dir))

    def load_from_file(self, file_name, **query_kwargs):
        """
        Load a template from a file within the templates_dir
        :param file_name: the name of the file
        :param query_kwargs: Optional keyword arguments
        :return: rendered template
        """
        return self.template_loader.get_template(file_name).render(**query_kwargs)
