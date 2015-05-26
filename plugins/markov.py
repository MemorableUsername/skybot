from util import hook
 
import random, os, re, math
from collections import Counter

# The max chain length determines the maximum complexity of markov chains formed
# during word analysis. Larger numbers will cause more realistic chains to be formed, provided
# enough text, but will slow down analysis and message generation.
max_chain_length_to_record = 3

# This is the chainID the parser considers the "terminating" word. During sentence generation
# if a chain ends in this word, the sentence is considered finished.
terminating_chainID = 0

# If this is set to true, the plugin will print out a message every time it manipulates the database
# to associate chains. Due to the sheer number of chain associations in a single sentence this
# can quickly flood the logs, so only enable this if you're trying to figure out how stuff works.
log_updates_to_console = True

# response_rate determines the frequency at which the bot will randomly respond to an
# observed message when not directly addressed. This occurs after recording the patterns
# within the prompting message. A value of 1.0 represents a 100% response rate, whereas
# a value of 0 will stop the bot from randomly responding entirely. 
response_rate = 0.0


channel_pattern = re.compile('-(?:c|-chan(?:nel)?) +(#[^ ]+|\*)', re.IGNORECASE)
nickname_pattern = re.compile('-(?:n|-nick(?:name)?) +(#?[a-z0-9-_^]+)', re.IGNORECASE)
chain_length_pattern = re.compile('-(?:l|-length) +([1-%d])' % max_chain_length_to_record, re.IGNORECASE)
# the .markov <user> command
@hook.command('mkv')
@hook.command
def markov(inp, nick='', chan='', db=None):
    ".mkv/.markov <[-n,--nick,--nickname NICKNAME] [-c,--chan,--channel #CHANNEL] [-l,--length CHAIN_LENGTH]> -- " \
        "generates a sentence in the style of NICKNAME (default: any) in CHANNEL (default: current), " \
        "based on previously observed word association patterns matching this criteria. " \
        "The CHAIN_LENGTH (default: 2, min:1, max: 3) argument determines markov chain length."
    
    channel_match = re.search(channel_pattern, inp)
    channel_str = channel_match.group(1) if channel_match else chan
    
    nick_match = re.search(nickname_pattern, inp)
    nick_str = nick_match.group(1) if nick_match else None
    
    chain_length_match = re.search(chain_length_pattern, inp)
    chain_length_str = chain_length_match.group(1) if chain_length_match else '2'
 
    # generate sentence
    sentence = construct_sentence(nick_str, channel_str, db, int(chain_length_str))
    
    return sentence
 
 
 
# builds the sentence
def construct_sentence(nick, chan, db, chain_length=None):
    chosen_chainID = obtain_root_chain_pairing(nick, chan, db, chain_length)
    component_substrings = []
    while chosen_chainID != 0:
        try:
            this_substr, chosen_chainID = obtain_words_and_next_chainID(chosen_chainID, nick, chan, db)
        except:
            import traceback
            traceback.print_exc()
            return
        component_substrings.append(this_substr)
    return ' '.join(component_substrings)

# The plugin will watch the chat record word patterns.
@hook.regex("^[^.?]")
def watch_chat(inp, nick='', chan='', db=None, match='', msg='', say=''):
    # try to create necessary tables
    #markov_words maps each word to a wordID
    db.execute("CREATE TABLE IF NOT EXISTS markov_words (wordID INTEGER PRIMARY KEY, value UNIQUE)")
    #markov_chain_data maps chains of words to a single chainID
    db.execute("CREATE TABLE IF NOT EXISTS markov_chain_data (chainID INTEGER PRIMARY KEY, component_wordIDs UNIQUE)")
    #markov_chain_association_data maps each chainID to another other chainID, with data on the context and how many times this relation has been observed
    db.execute("CREATE TABLE IF NOT EXISTS markov_chain_association_data (chainID, next_chainID, count_observed, channel_observed, nickname_observed)")
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_non_count ON markov_chain_association_data(chainID, next_chainID, channel_observed, nickname_observed)")
    db.commit()
    
    allchains = build_allchains_dict_from_message(msg, db)
    
    # make a list of all the component_wordID strings as these uniquely identify chains        
    component_wordID_list = [chain['component_wordIDs'] for chain_list in allchains.values() for chain in chain_list]
    # convert list to set, then back to list to remove duplicates and shorten query
    cwid_set_list = list(set(component_wordID_list))
    # make any new chains for observed component_wordIDs into the table of known chains, then obtain the IDs of the chains in use
    chainID_dict = store_new_chains(cwid_set_list, db)
    
    # print stuff to console if logging is enabled
    if log_updates_to_console:
        print '### INFO: markov plugin observed chains in channel %s from nickname %s:' % (chan, nick)
        for k in allchains.keys():
            print '###   length %d:' % k
            for d in allchains[k]: print '###     %s -%s> %s' % (d['chain words'], '-'*(68-len(str(d['chain words']))-len(d['component_wordIDs'])), d['component_wordIDs'])

    # now it's time to associate chains with each other
    association_counter = Counter()
    
    for chain_length_listing in allchains:
        origin = '(origin)'
        origin_ID = -1
        first = allchains[chain_length_listing][0]
        first_chainID = chainID_dict[first['component_wordIDs']]
        if log_updates_to_console:
            print 'association: %s -> %s [%s -> %s]' % (str(origin), str(first), origin_ID, first_chainID)
        association_counter.update(((origin_ID, first_chainID),))

        chain_list_length = len(allchains[chain_length_listing])
        for chain_index in xrange(chain_list_length):
            me = allchains[chain_length_listing][chain_index]
            own_chainID = chainID_dict[me['component_wordIDs']]
            associated_idx = chain_index + chain_length_listing
            if associated_idx < len(allchains[chain_length_listing]):
                association = allchains[chain_length_listing][associated_idx]
                associated_chainID = chainID_dict[association['component_wordIDs']]
            else:
                association = None
                associated_chainID = 0
            if log_updates_to_console:
                print 'association: %s -> %s [%s -> %s]' % (str(me), str(association), own_chainID, associated_chainID)
            association_counter.update(((own_chainID, associated_chainID),))
    
    associate_chains_to_ID(association_counter, db, chan, nick)
    db.commit()
    
    if random.random() < response_rate:
        say(construct_sentence(nick=None, chan=chan, db=db, chain_length = 2))
    return None

def build_allchains_dict_from_message(msg, db):
    word_list = msg.split()
    # convert list to set, then back to list to remove duplicates and shorten queries
    word_set_list = list(set(word_list))
    # add each word into the table of known words, then obtain the IDs of these words

    store_new_words(word_set_list, db)
    wordID_dict = get_IDs_for_words(word_set_list, db)
    
    # now generate chains
    allchains = parse_observed_word_list(word_list, wordID_dict)
    return allchains

def parse_observed_word_list(word_list, wordID_dict):
    word_count = len(word_list)
    allchains = {}
    #examine each word in the sequence, building tree out for each word
    max_possible_chain_length_here = min(max_chain_length_to_record, word_count)
    for i in xrange(word_count):
        # make the chains for each word
        for chain_number in xrange(max_possible_chain_length_here):
            if not allchains.get(chain_number+1): allchains[chain_number+1] = []
            chain = []
            #if i - chain_number < 0: chain.append(object)
            # build each chain of increasing length branching off of this word, up to either the end of the message or max_chain_length
            for j in xrange(chain_number+1):
                idx = i+j
                if idx < len(word_list): chain.append(word_list[idx])
                else:
                    chain.append(None)
                    break
            component_wordIDs = ','.join([str(wordID_dict.get(word, 0)) for word in chain])
            
            allchains[chain_number+1].append({'chain words':chain, 'component_wordIDs':component_wordIDs})
    return allchains

def store_new_words(word_set_list, db):
    # add each word into the table of known words; wordID will be automatically assigned
    db.execute("insert or ignore into markov_words (value) values %s" % ','.join(['(?)']*len(word_set_list)), word_set_list)
    db.commit()

def get_IDs_for_words(word_set_list, db):
    # find the newly generated IDs of these words
    wordID_dict = {i[0]:i[1] for i in db.execute("select value, wordID from markov_words where value in (%s)" % ','.join(['?']*len(word_set_list)), word_set_list).fetchall()}
    return wordID_dict

def obtain_root_chain_pairing(nick, chan, db, length):
    base_query = "select next_chainID from markov_chain_association_data " \
             "where chainID = -1"
    base_args = []
    if length == None: length = 1
    if nick != None:
        base_query += " and nickname_observed = (?)"
        base_args.append(nick)
    if chan != None:
        base_query += " and channel_observed = (?)"
        base_args.append(chan)
    #all_roots_matching_criteria = {e[0]:e[1] for e in db.execute(base_query, base_args).fetchall()}
    #chainID_counter = Counter(all_roots_matching_criteria)
    
    
    
    if length == 1:
        narrowing_prefix = "like '%%'"
    elif length > 1:
        narrowing_prefix = "like '%s'" % ','.join(['%']*length)
    narrowing_suffix = "not like '%s'" % ','.join(['%']*(length+1))
    length_constraint_query = "select chainID from markov_chain_data where chainID in (%s) and component_wordIDs %s and component_wordIDs %s" % (base_query, narrowing_prefix, narrowing_suffix)
    c = Counter({e[0]:e[1] for e in db.execute("select next_chainID, count_observed from markov_chain_association_data where next_chainID in (%s)" % (length_constraint_query), base_args).fetchall()})
    i = random.choice(list(c.elements()))
    return i

def obtain_words_and_next_chainID(current_chainID, nick, chan, db):
    #first get component words, I screwed up the schema here
    #need to split result programmatically, this is why this approach will fail with large chains in the future

    component_wordIDs = db.execute("select component_wordIDs from markov_chain_data where chainID = (?)", (current_chainID,)).fetchone()[0].split(',')
    real_word_list = []
    for wordID in component_wordIDs:
        if wordID != '0': real_word_list.append(db.execute("select value from markov_words where wordID = (?)", (wordID,)).fetchone()[0])
    substr = ' '.join(real_word_list)
    
    #now, find all chains that branch off this one and choose one at random
    base_query = "select next_chainID, count_observed from markov_chain_association_data " \
             "where chainID = (?)"
    base_args = [current_chainID]
    if nick != None:
        base_query += " and nickname_observed = (?)"
        base_args.append(nick)
    if chan != None:
        base_query += " and channel_observed = (?)"
        base_args.append(chan)
    
    
    c = Counter({e[0]:e[1] for e in db.execute(base_query, base_args).fetchall()})
    if len(c) > 0: i = random.choice(list(c.elements()))
    else: i = 0
    
    return substr, i
    

def store_new_chains(cwid_set_list, db):
    # add each component_wordID string into the table of known chains; chainID will be automatically assigned
    db.execute("insert or ignore into markov_chain_data (component_wordIDs) values %s" % ','.join(['(?)']*len(cwid_set_list)), cwid_set_list)
    # find the newly generated IDs of these chains
    chainID_dict = {i[0]:i[1] for i in db.execute("select component_wordIDs, chainID from markov_chain_data where component_wordIDs in (%s)" % ', '.join(['?']*len(cwid_set_list)), cwid_set_list).fetchall()}
    return chainID_dict

#schema: chainID, next_chainID, count_observed, channel_observed, nickname_observed
def associate_chains_to_ID(association_counter, db, chan, nick):
    #existing_associations = db.execute("SELECT * FROM markov_chain_association_data WHERE channel_observed IS (?) AND nickname_observed IS (?) AND (%s)" %
    #                                   ' OR '.join(['(chainID IS (?) AND next_chainID IS (?))']*len(association_counter)), [chan, nick] + [x for pair in association_counter.keys() for x in pair]).fetchall()
    
    # increment existing associations
    
    
    db.execute("INSERT OR REPLACE INTO markov_chain_association_data " \
                        "(chainID, next_chainID, count_observed, channel_observed, nickname_observed) " \
                        "VALUES %s" % 
                       ','.join(["""(?, ?, COALESCE ((SELECT count_observed FROM markov_chain_association_data WHERE chainID IS ? AND next_chainID IS ? AND channel_observed IS ? AND nickname_observed IS ?)+1, 1), ?, ?)"""]*len(association_counter.keys())), [t for pair in association_counter.keys() for t in [pair[0], pair[1], pair[0], pair[1], chan, nick, chan, nick]])
    #previous_observed = db.execute("SELECT count_observed from markov_chain_association_data where chainID is (?) and next_chainID is (?) and channel_observed is (?) and nickname_observed is (?)", (chainID, next_chainID, chan, nick)).fetchall()
    
    
    
    #if not previous_observed: db.execute("insert into markov_chain_link_data (channel, nickname, next_chainID, observed, component_wordIDs) values (?, ?, ?, ?, ?)", (chan, nick, lwID, 1, component_wordIDs))
    #else: db.execute("update markov_chain_link_data set observed=(?) where channel is (?) and nickname is (?) and component_wordIDs is (?) and next_chainID is (?)", (previous_observed[0]+1, chan, nick, component_wordIDs, lwID))
    
    #this_chainID = db.execute("select chainID from markov_chain_link_data where channel is (?) and nickname is (?) and component_wordIDs is (?) and next_chainID is (?)", (chan, nick, component_wordIDs, lwID)).fetchone()[0]
    #if log_updates_to_console: print '### INFO: markov plugin updated markov_chain_association_data with entry for %s: chainID=%d, channel=%s, nickname=%s, component_wordIDs=%s, next_chainID=%d, previous_observed (before incriment)=%s' % (str(chain_list), this_chainID, chan, nick, component_wordIDs, lwID, str(previous_observed))
    #return this_chainID
