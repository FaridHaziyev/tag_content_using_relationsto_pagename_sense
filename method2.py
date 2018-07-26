from xml.dom.minidom import parse, parseString
from nltk.corpus import wordnet,stopwords
from nltk.stem.wordnet import WordNetLemmatizer
import pdb
import re
import pickle
import multiprocessing as mp

stopWords = set(stopwords.words("english"))
lmtzr = WordNetLemmatizer()

#pages tagged using first method
with open("german_tagged_pages.pkl", "rb") as fr:
    german_pages = pickle.load(fr)


with open("../merged_eng_deu_dict.pkl", "rb") as ed:
    eng_german_dict = pickle.load(ed)

def load_xml_data(data_name):
    wikipairs_data = parse(data_name)
    return wikipairs_data

def get_content(page, language):
    #here prepositions should be removed
    contents = page.getElementsByTagName(language)[0].firstChild.data
    contents = contents.replace("\n", " ").replace("\t", " ")
    contents = re.findall("\w+", contents)
    return contents  

def get_all_pairs(xml_tree):
    all_pairs = xml_tree.getElementsByTagName("Pair")
    return all_pairs


def get_page_name(page, source = False):
    if not source:
        page_ = page.getElementsByTagName("Actual_Name")[1]
    else:
        #to get the name of the german part
        page_ = page.getElementsByTagName("Actual_Name")[0]

    return page_.firstChild.data


def get_synset_id(synset):
    return (8-len(str(synset.offset())))*"0" + str(synset.offset()) + "-" + synset.pos()

def get_sense_context(synset):
    relations = set()
    relations.update(synset.hyponyms())
    relations.update(synset.hypernyms())
    relations.update(synset.part_meronyms())
    relations.update(synset.substance_meronyms())
    relations.update(synset.member_meronyms())
    relations.update(synset.member_holonyms())
    
    return relations


def tag_using_relations(pages,german_tags):
    for page in pages:
        tagged_words = dict()
        p_name = get_page_name(page, source = True).lower()
        #check if the name of the page is tagged
        if p_name in german_pages:
            #get the tag of the page
            p_synset = wordnet.synset(german_pages[p_name])
            #get the context of the synset
            sense_context = get_sense_context(p_synset)
            #get the english content of the page
            page_content = get_content(page, "English_Content")
            for word in page_content:
                if len(word) < 3:
                    continue
                #get pos synsets of the word
                pos_synsets = wordnet.synsets(word)
                if pos_synsets:
                    for s in pos_synsets:
                        #check if sense is in the page's context
                        if s in sense_context:
                            tagged_words[word] = get_synset_id(s)
                            break
            source_content = get_content(page, "German_Content")

        for word in tagged_words:
            if word not in eng_german_dict:
                continue
            translations = eng_german_dict[word]
            selected = [0,None]
            for translation in translations:
                countt = source_content.count(translation)
                if countt > selected[0]:
                    selected[1] = translation
            if selected[1] != None:
                german_tags.append((selected[1], tagged_words[word]))
            

if __name__ == "__main__":   
    wikipages = load_xml_data("../created_datas/wikipair_de_en.xml")  #to load wikipages
    wikipairs = get_all_pairs(wikipages) #to get all wikipairs
    
    manager = mp.Manager()
    ds = manager.list()

    #tag_using_relations(wikipairs, ds)

    n = 80
    p_length = len(wikipairs) // n
    threads = []
    output = dict()
    for i in range(n):
        start = i*p_length
        if i == n - 1:
            end = len(wikipairs)           
            threads.append(mp.Process(target=tag_using_relations, 
                                          args = (wikipairs[start:end],ds,)))


        else:
            end = i*p_length + p_length
            threads.append(mp.Process(target=tag_using_relations, 
                                          args = (wikipairs[start:end],ds,)))

    a = 4
    b = 20
    print("started")
    for x in range(a):
        for y in range(b):
            threads[x*b+y].start()
        for z in range(b):
            threads[x*b+z].join()
   

    german_todict = dict()
    for page_name, synset in ds:
        if page_name not in german_todict:
            german_todict[page_name] = set()
        german_todict[page_name].add(synset)

    with open("german_rel_tagged.txt", "w") as fw:
        for p_name, syns in german_todict.items():
            print(p_name + "\t" + str(syns), file = fw)



