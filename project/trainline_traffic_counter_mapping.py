
'''Returns mapping from a trainline to the relevant traffic counters.
In this context, relevant means traffic counters that capture possible 
alternative streets to the specified trainline'''
def generate_trainline_counter_mapping():
    train_counter_mapping = {
        'A 1: Neumünster - Eidelstedt':         [],
        'A 2: Ulzburg Süd - Norderstedt':       [],
        'A 3: Elmshorn - Ulzburg Süd':          [],
        'RB 61: Itzehoe - Hamburg (Hbf)':       [1154, 1119, 1167, 1197],
        'RB 62: Heide - Itzehoe':               [1270, 1269, 1163, 1268, 1197, 1178],
        'RB 63: Büsum - Neumünster':            [1117, 1125, 1270, 1269, 1163, 1268, 1187, 1183],
        'RB 64: St.-Peter-Ording - Husum':      [1175, 1132],
        'RB 65: Niebüll - Dagebüll':            [],
        'RB 66: Esbjerg - Niebüll':             [1133],
        'RB 71: Wrist - Hamburg-Altona':        [1173, 1119],
        'RB 73: Eckernförde - Kiel':            [1116],
        'RB 75: Rendsburg - Kiel':              [1162, 1194],
        'RB 76: Schönberger Strand - Kiel':     [1158],
        'RB 81: Bad Oldesloe - Hamburg (Hbf)':  [1190, 1102],
        'RB 82: Neumünster - Bad Oldesloe':     [1186, 1166],
        'RB 84: Kiel - Lübeck':                 [1194, 1104, 1106, 1165, 1166, 1164, 1170, 1108, 1135, 1156, 1149],
        'RB 85: Puttgarden - Lübeck':           [1131, 1124, 1105, 1108],
        'RB 86: Travemünde - Lübeck':           [1108],
        'RE 1: Hamburg (Hbf) - Büchen':         [1172, 1138],
        'RE 1: Hamburg (Hbf) - Schwerin':       [1172, 1138, 1110, 1706, 1160],
        'RE 4: Lübeck - Bad Kleinen':           [],
        'RE 6: Westerland - Hamburg-Altona':    [],
        'RE 7: Flensburg - Hamburg (Hbf)':      [],
        'RE 7: Flensburg/Kiel - Hamburg (Hbf)': [],
        'RE 70: Kiel - Hamburg (Hbf)':          [],
        'RE 72: Flensburg - Kiel':              [],
        'RE 74: Husum - Kiel':                  [],
        'RE 8: Lübeck - Hamburg (Hbf)':         [],
        'RE 80: Lübeck - Hamburg (Hbf)':        [],
        'RE 83: Kiel - Lübeck':                 [],
        'RE 83: Lübeck - Lüneburg':             [],
        'RE 85: Puttgarden - Hamburg (Hbf)':    []
    }
    return train_counter_mapping


'''Returns the reverse mapping to generate_train_counter_mapping(), meaning:
Maps a traffic counter to all trainlines, for which the counter is relevant.
'''
def generate_counter_trainline_mapping():
    # Get all relevant traffic counters from all trainlines
    all_traffic_counters = []
    train_counter_mapping = generate_trainline_counter_mapping()
    for trainline_counters in train_counter_mapping.values():
        all_traffic_counters += trainline_counters
        
    # Make the counters unique
    all_traffic_counters = list(set(all_traffic_counters))
    
    # For every counter, go through the trainline->counters mapping and
    # check if the counter of interest is contained
    counter_trainline_mapping = {}
    for counter in all_traffic_counters:
        trainlines = []
        for trainline, trainline_counters in train_counter_mapping.items():
            if counter in trainline_counters:
                trainlines.append(trainline)
        
        counter_trainline_mapping.update({counter: trainlines})
                
    return counter_trainline_mapping