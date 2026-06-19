import re
import jellyfish

class BusquedaFirmaAutor:
    def __init__(self):
        # A simple tokenizer that splits names into lowercase word tokens
        self.tokenize = lambda s: re.sub(r'[^\w\s]', ' ', s.lower()).split()
        self.candidatos = []

    def _internal_token_similarity(self, token_a, token_b):
        """
        The internal metric used by Monge-Elkan.
        Combines Jaro-Winkler with a rule for academic initials.
        """
        # Rule 1: High partial match if one token is a valid initial of the other
        if len(token_a) == 1 and token_b.startswith(token_a):
            return 0.85
        if len(token_b) == 1 and token_a.startswith(token_b):
            return 0.85
            
        # Rule 2: Default to standard Jaro-Winkler for typos or full words
        return jellyfish.jaro_winkler_similarity(token_a, token_b)

    def monge_elkan_similarity(self, name_a_tokens, name_b_tokens, direction='forward'):
        """
        Pure Python implementation of the Monge-Elkan Algorithm.
        For every token in A, finds the best matching token in B, 
        and averages the results.
        """
        if not name_a_tokens or not name_b_tokens:
            return 0.0
            
        sum_max_similarities = 0.0
        
        if direction == 'backward':
            name_a_tokens, name_b_tokens = name_b_tokens, name_a_tokens
        
        for token_a in name_a_tokens:
            max_sim = 0.0
            for token_b in name_b_tokens:
                sim = self._internal_token_similarity(token_a, token_b)
                if sim > max_sim:
                    max_sim = sim
            sum_max_similarities += max_sim
            
        return sum_max_similarities / len(name_a_tokens)

    def find_author_in_paper(self, target_name, paper_authors, direction='forward'):
        """Compares target full name to a small list of paper co-authors."""
        target_tokens = self.tokenize(target_name)
        match_results = []
        
        for author in paper_authors:
            author_tokens = self.tokenize(author)
            
            # 1. Calculate Monge-Elkan Similarity
            score = self.monge_elkan_similarity(target_tokens, author_tokens, direction=direction)

            match_results.append({
                "signature": author,
                "score": round(score, 2)
            })
            
        # Sort results by highest likelihood
        match_results.sort(key=lambda x: x['score'], reverse=True)

        self.candidatos = match_results

    def get_best_match(self, min_score=0.0):
        """Returns the best match from the last search."""
        if len(self.candidatos) == 0:
            return None
        
        if len(self.candidatos) > 1 and self.candidatos[0]['score'] == self.candidatos[1]['score']:
            return None
        
        if self.candidatos[0]['score'] < min_score:
            return None

        return self.candidatos[0]