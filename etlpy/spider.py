# coding=utf-8

import sys;
import re
import requests
from lxml import etree
from itertools import groupby
from etlpy.extends import EObject, to_str, PY2, get_variance, is_str
box_regex = re.compile(r"\[\d{1,3}\]");
agent_list = []


class XPath(EObject):
    def __init__(self, name=None, xpath=None, is_html=False, sample=None, must=False):
        self.path = xpath;
        self.sample = sample;
        self.name = name;
        self.must = must;
        self.is_html = is_html;
        self.children = [];

    def __str__(self):
        return "%s %s %s" % (self.name, self.path, self.sample);


def xpath_rm_last_num(paths):
    v = paths[-1];
    m = box_regex.search(v);
    if m is not None:
        s = m.group(0);
        paths[-1] = v.replace(s, "");
    return '/'.join(paths);


def get_common_xpath(xpaths):
    paths = [r.path.split('/') for r in xpaths];
    minlen = min(len(r) for r in paths);
    c = None;
    for i in range(minlen):
        for index in range(len(paths)):
            path = paths[index];
            if index == 0:
                c = path[i];
            elif c != path[i]:
                first = path[0:i + 1];
                return xpath_rm_last_num(first);


def xpath_take_off(path, root_path):
    r = path.replace(root_path, '');
    if r.startswith('['):
        r = '/'.join(r.split('/')[1:])
    return r


def xpath_iter_sub(path):
    xps = path.split('/');
    for i in range(2, len(xps)):
        xp = xpath_rm_last_num(xps[:i])
        yield xp;


attrsplit = re.compile('@|\[');


def get_xpath_data(node, path, is_html=False, only_one=True):
    p = node.xpath(path);
    if p is None:
        return None;
    if len(p) == 0:
        return None;
    paths = path.split('/');
    last = paths[-1];
    attr = False;
    if last.find('@') >= 0:  # and last.find('[1]')>=0:
        attr = True;
    results = [];

    def get(x):
        if attr:
            return to_str(x)
        elif is_html:
            return etree.tostring(x).decode('utf-8')
        else:
            return get_node_text(x);

    for n in p:
        result = get(n)
        if only_one:
            return result;
        results.append(result);
    return results;


_extract = re.compile('\[(\w+)\]');
_charset = re.compile('<meta[^>]*?charset="?(\\w+)[\\W]*?>');
_charset = re.compile('charset="?([A-Za-z0-9-]+)"?>');

default_encodings = ['utf-8', 'gbk'];


def get_encoding(html):
    encoding = _charset.search(to_str(html))
    if encoding is not None:
        encoding = encoding.group(1);
    if encoding is None:
        encoding = 'utf-8'
    except_encoding = encoding
    try:
        result = html.decode(except_encoding)
        return result;
    except UnicodeDecodeError as e:
        pass;

    for en in default_encodings:
        if en == except_encoding:
            continue;
        try:
            result = html.decode(en)
            return result;
        except UnicodeDecodeError as e:
            continue;
    sys.stderr.write(str(e) + '\n');
    import chardet
    en = chardet.detect(html)['encoding']
    result = html.decode(en, errors='ignore');
    return result


def get_html(url):
    r = requests.get(url)
    return r.text;


def is_none(data):
    return data is None or data == '';


def __get_node_text(node, array):
    if hasattr(node, 'tag') and isinstance(node.tag, str) and node.tag.lower() not in ['script', 'style', 'comment']:
        t = node.text;
        if t is not None:
            t = t.strip()
            if t != '':
                array.append(t)
        t = node.tail;
        if t is not None:
            t = t.strip()
            if t != '':
                array.append(t)
        for sub in node.iterchildren():
            __get_node_text(sub, array)


def get_node_text(node):
    if node is None:
        return ""
    array = [];
    __get_node_text(node, array);
    return ' '.join(array);


def get_node_html(node):
    if node is None:
        return ""
    if str(type(node)).lower().find('str') > 0:
        return str(node)
    else:
        return etree.tostring(node).decode('utf-8');


if PY2:
    pass
else:
    import html.parser as h

    html_parser = h.HTMLParser()

PM25 = 2.4;

ignoretag = re.compile('script|style');
boxRegex = re.compile(r"\[\d{1,3}\]");


def str_match(text, keyword, match_func):
    if text is None: return 0;
    keyword = keyword.strip();
    items = [r.strip() for r in re.split('\t|\r|\n', text)];
    for r in items:
        if match_func(r, keyword):
            return 1;
    return 0;


def get_node_leaf_count(node):
    count = 0;
    if node is None: return;
    nodes = [r for r in node.iterchildren()];
    c = len(nodes);
    if c == 0: count += 1;
    for node in nodes:
        count += get_node_leaf_count(node);
    return count;


def _is_same_path(p1, p2, root_path):
    path1 = p1.replace(root_path, '');
    path2 = p2.replace(root_path, '');
    return str(path1) == str(path2);


def get_diff_page(htmls, has_attr):
    trees = []
    nodes = [];
    for html in htmls:
        root = etree.HTML(html);
        tree = etree.ElementTree(root);
        nodes.append(root);
        trees.append(tree);
    xpaths = [];
    __get_diff_nodes(trees, nodes, xpaths, has_attr);
    return xpaths;


def search_text_root(tree, node):
    class ParaClass(object):
        def __init__(self):
            self.tlen = 0;
            self.node = ''

    para = ParaClass();
    __search_text_root(tree, None, node, para);
    return para.node


def __search_text_root(tree, father, node, para):
    if hasattr(node, 'tag') and isinstance(node.tag, str) and node.tag.lower() not in ['script', 'style', 'comment']:
        child_nodes = [n for n in node.iterchildren()];
        if len(child_nodes) > 0:
            for child in child_nodes:
                __search_text_root(tree, node, child, para);
        else:
            text = node.text;
            if text == None:
                return;
            tlen = len(text)
            if tlen > para.tlen:
                para.tlen = tlen;
                para.node = father


def __get_sub_xpath(path, slice):
    r = path.split('/');
    paths = slice(r)
    return '/'.join(paths)


def __search_table_root(tree, nodes, path_dict, has_child, strict=True):
    if strict:
        variance_max = 2
    else:
        variance_max = 10

    if nodes is None:
        return;
    if len(nodes) == 0:
        return None;
    node = nodes[0];
    if has_child:
        for node in nodes:
            all_childs = [child for child in node.iterchildren() if str(child.__class__).find('Element') > 0]
            childs = groupby(all_childs, key=lambda node: node.tag);
            for key, node_group in childs:
                node_group = list(node_group);
                __search_table_root(tree, node_group, path_dict, has_child, strict);
    target_node = list(node for node in nodes);  # filter(lambda x:not x.tag.startswith("#"),nodes);
    child_count = float(len(target_node));
    if 5 > child_count:
        return;
    same_name_count = len([x for x in target_node if x.tag == target_node[1].tag]);
    if same_name_count < child_count * 0.7: return;
    child_counts = [];
    for n in target_node:
        child_counts.append(len(list(r for r in n.iterchildren())));
    variance = get_variance(child_counts);
    if variance > variance_max: return;
    leaf_count = get_node_leaf_count(target_node[0]);
    if leaf_count < 2:
        return;
    value = child_count * PM25 + leaf_count;
    xpath = xpath_rm_last_num(tree.getpath(node).split('/'));
    path_dict[xpath] = value;


def search_table_root(root, has_child=True):
    d = {};
    tree = etree.ElementTree(root);
    __search_table_root(tree, root, d, has_child);
    return d;


def _str_find(string, word):
    return string.find(word) >= 0;


def _regex_find(string, regex):
    res = re.match(regex, string);
    if res:
        return res;
    return res;


def _tn_find(string, rule):
    from tn import core
    return core.match(string, rule) is not None;


def search_xpath(node, keyword, match_func='str', has_attr=False):
    tree = etree.ElementTree(node);
    dics = {'str': _str_find, 'tn': _tn_find, 'script': _regex_find};
    return __search_xpath(tree, node, keyword, dics[match_func], has_attr);


def __search_xpath(tree, node, keyword, match_func, has_attr=False):
    if node is None or keyword is None: return;
    nodes = node.iterchildren();
    for node in nodes:
        if str(node.__class__).find("Element") > 0:
            path = __search_xpath(tree, node, keyword, match_func, has_attr);
            if path is not None:
                return path;
            if node.text is not None and str_match(node.text, keyword, match_func):
                xpath = tree.getpath(node)
                return xpath;
            if has_attr:
                for r in node.attrib:
                    if str_match(node.attrib[r], keyword, match_func):
                        xpath = tree.getpath(node) + '/@%s[1]' % (r)
                        return xpath;
    return None


def __get_nearest_node(targets, node):
    dic = {};
    for target in targets:
        if type(target) != type(node):
            continue;
        if target.tag != node.tag:
            continue;
        dis = len(target.attrib.keys()) - len(node.attrib.keys())
        dis = abs(dis);
        dis += abs(get_node_leaf_count(target) - get_node_leaf_count(node))
        dic[target] = dis;
    minv = 99999;
    selected_node = None;
    for k, v in dic.items():
        if v < minv:
            selected_node = k;
    return selected_node;


def __get_diff_nodes(trees, nodes, xpaths, has_attr):
    def get_tree(i):
        if isinstance(trees, list):
            return trees[i];
        return trees;

    is_child_contain_info = False;
    index = int(len(nodes) / 2);
    node1 = nodes[index]
    tree1 = etree.ElementTree(node1);
    node1path = get_tree(0).getpath(node1);
    for child_node1 in node1.iterchildren():
        path = '/'.join(tree1.getpath(child_node1).split('/')[2:])
        node_child2 = [];
        for node in nodes:
            targets = node.xpath(path);
            if len(targets) == 0:
                continue  # TODO: this is fucked
            node_child2.append(targets[0]);
        if len(node_child2) <= 1:
            continue;
        is_child_contain_info |= __get_diff_nodes(trees, node_child2, xpaths, has_attr);
    if is_child_contain_info == False:
        for i in range(0, len(nodes)):
            node = nodes[i];
            if not __is_same_string(node.text, node1.text):
                prop_name = __search_node_name(node, xpaths);
                xpath = XPath(prop_name)
                xpath.sample = node1.text;
                xpath.path = node1path if len(xpaths) % 2 == 0 else get_tree(i).getpath(node);
                xpaths.append(xpath);
                is_child_contain_info = True;
                break;
    if not has_attr:
        return is_child_contain_info;
    for r in node1.attrib:
        v = node1.attrib[r];
        for i in range(0, len(nodes)):
            node = nodes[i];
            value = node.attrib.get(r, None);
            if value is None:
                break;
            if node.attrib[r] != v:
                xpath = XPath(__search_node_name(r, xpaths) + "_" + r);
                xpath.path = node1path if len(xpaths) % 2 == 0 else get_tree(i).getpath(node);
                xpath.path += '/@' + r;
                xpath.sample = v;
                xpaths.append(xpath);
                break;
    return is_child_contain_info;


def get_diff_nodes(tree, root, root_path, has_attr, exists=None):
    xpaths = [];
    nodes = [r for r in root.xpath(root_path)]
    count = len(nodes);
    if count > 1:
        __get_diff_nodes(tree, nodes, xpaths, has_attr);
    if exists is not None:
        for r in exists:
            for p in xpaths:
                short_path = xpath_take_off(p.path, root_path);
                if r.path == to_str(short_path):
                    p.name = r.name;
                    break;
    return xpaths;


def __is_same_string(t1, t2):
    if t1 is None and t2 is None:
        return True;
    elif t1 is not None and t2 is not None:
        return t1.strip() == t2.strip();
    return False


def __search_node_name(node, xpaths):
    if not hasattr(node, 'attrib'):
        return 'col%s' % (len(xpaths))
    attr_key = ["class", "id"]
    for key in attr_key:
        name = node.attrib.get(key, None)
        if name is not None:
            break;
    if name is None or name == '':
        return 'col%s' % (len(xpaths))
    for c in xpaths:
        if c.name == name:
            name2 = node.getparent().attrib.get(name, None)
            if name2 is None:
                return 'col%s' % (len(xpaths))
            else:
                name = name2 + '_' + name
    return name.replace(' ', '_')


def search_properties(root, exist_xpaths=None, is_attr=False):
    if exist_xpaths == None: exist_xpaths = [];
    tree = etree.ElementTree(root)
    exist_len = len(exist_xpaths)
    if exist_len > 1:
        root_path = get_common_xpath(exist_xpaths);
        yield root_path, get_diff_nodes(tree, root, root_path, is_attr, exist_xpaths);
    elif exist_len == 1:
        real_path = exist_xpaths[0];
        path_dict = {};
        for r in xpath_iter_sub(real_path.path):
            __search_table_root(tree, root.xpath(str(r)), path_dict, False, strict=False);
        max_p = 0;
        path = None;
        for r in path_dict:
            if path_dict[r] > max_p:
                path = r;
            max_p = path_dict[r];
        if path is not None:
            items = get_diff_nodes(tree, root, path, is_attr, exist_xpaths);
            if len(items) > 1:
                yield path, items
    else:
        path_dict = {};
        __search_table_root(tree, [root], path_dict, True);
        path_dict = sorted(path_dict, key=lambda d: path_dict[d], reverse=True);
        for root_path in path_dict:
            items = get_diff_nodes(tree, root, root_path, is_attr, exist_xpaths);
            if len(items) > 1:
                yield root_path, items;
    yield None, None


def _get_etree(html):
    root = None
    if html != '':
        try:
            root = etree.HTML(html);
        except Exception as e:
            sys.stderr.write('html script error' + str(e))
    return root;


def get_list(html, xpaths=None, has_attr=False):
    root = _get_etree(html);
    if xpaths is None:
        root_path, xpaths = search_properties(root, None, has_attr);
    datas = get_datas(root, xpaths, True, None)
    return datas, xpaths;


def get_main(html):
    if is_str(html):
        html = _get_etree(html);
    tree = etree.ElementTree(html);
    node = search_text_root(tree, html);
    return node


def get_sub_xpath(root, xpath):
    paths = xpath.split('/');
    path = '/' + '/'.join(paths[len(root.split('/')):len(paths)]);
    return path;


def get_datas(root, xpaths, multi=True, root_path=None):
    tree = etree.ElementTree(root);
    docs = [];
    if not multi:
        doc = {};
        for r in xpaths:
            data = get_xpath_data(tree, r.path, r.is_html);
            if data is not None:
                doc[r.name] = data;
            else:
                doc[r.name] = "";
        return doc;
    else:
        if is_none(root_path):
            root_path2 = get_common_xpath(xpaths);
        else:
            root_path2 = root_path;
        nodes = tree.xpath(root_path2)
        if nodes is not None:
            for node in nodes:
                doc = {};
                for r in xpaths:
                    path = r.path;
                    if is_none(root_path):
                        paths = r.path.split('/');
                        path = '/'.join(paths[len(root_path2.split('/')):len(paths)]);
                    else:
                        path = tree.getpath(node) + path;
                    if path == '':
                        path = '/'
                    data = get_xpath_data(node, path, r.is_html);
                    if data is not None:
                        doc[r.name] = data;
                if len(doc) == 0:
                    continue;
                docs.append(doc);
            return docs;
