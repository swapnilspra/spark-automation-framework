from collections import defaultdict
import copy
import re
from typing import Dict, List, Any, Tuple, Pattern, Match, Optional

"""
Represents a single copy book field
"""
class Field:

    def __init__(self,
        name:str,
        datatype:str, # int, float, str, group
        length:int=0, # total length in file in bytes. if float, includes both decimal and precision
        level:int=0,
        precision:int=None, # if datatype=float, this is the number of decimal points
        signed:str=None,
        comp3:bool=False, # indicates whether or not packed with comp3 EBCDIC
        redefines:str=None, # redefines means that it shares storage space with another group
        occurs:int=1 # a group can occur more than once. a nested table
        ):
        self.name = name
        self.datatype = datatype
        self.length = length
        self.precision = precision
        self.signed = signed
        self.occurs = occurs
        self.comp3 = comp3
        self.level = level
        self.group = ''
        self.redefines = redefines
        self._children:List[Field] = []
        self.offset:int = -1

    def add_child(self,field):
        self._children.append(field)

    def __repr__(self):
        return str({"name":self.name,
            "datatype":self.datatype,
            "length":self.length,
            "precision":self.precision,
            "signed":self.signed,
            "comp3":self.comp3,
            "redefines":self.redefines,
            "offset":self.offset,
            "occurs":self.occurs
        })

    @staticmethod
    def from_copybook_str(definition):
        match = CopyBook.row_pattern.match(definition.strip())
        if not match:
            return None
        else:
            metadata:Dict[str,Any] = match.groupdict()

            if metadata['pic'] is not None:
                pic_datatype, pic_length, pic_precision, pic_signed = Field.parse_pic_string(metadata['pic'])
                signed:str = None
                if pic_signed:
                    # signed pics can be leading or trailing
                    if metadata["signed"]=="TRAILING":
                        signed = "trailing"
                    else:
                        # default is leading
                        signed = "leading"

                return Field(
                    name=metadata["name"],
                    datatype=pic_datatype,
                    length=pic_length,
                    precision=pic_precision,
                    signed=signed,
                    level=int(metadata["level"]),
                    comp3=match["comp3"] is not None
                )
            else:
                # we are a group
                # check if this is an OCCURS record type
                if metadata['occurs'] is not None:
                    # we need to later replicate this group N times and rename the lot of them
                    return Field(
                        name = metadata["name"],
                        datatype = "group",
                        level=int(metadata["level"]),
                        redefines=metadata["redefines"],
                        occurs=int(metadata['occurs'])
                    )
                else:
                    # regular group
                    return Field(
                        name = metadata["name"],
                        datatype = "group",
                        level=int(metadata["level"]),
                        redefines=metadata["redefines"]
                    )

    @staticmethod
    # Parse the pic string
    def parse_pic_string(pic_str):
        # handle repeating chars
        for match in CopyBook.pic_pattern_repeats.finditer(pic_str):
            expanded_str = match.group(1) * int(match.group(2))        
            pic_str = CopyBook.pic_pattern_repeats.sub(expanded_str, pic_str, 1)

        # Match to types
        if CopyBook.pic_pattern_float.match(pic_str):
            datatype = 'float'
        elif CopyBook.pic_pattern_integer.match(pic_str):
            datatype = 'int'
        else:
            datatype = 'str'

        signed = False
        # Handle signed
        if pic_str[0] == "S" or pic_str[0] == '-':
            signed = True
            pic_str = pic_str[1:]

        # Handle precision
        precision = 0

        if 'V' in pic_str:
            # implied decimal
            precision = len(pic_str[pic_str.index('V') + 1 :])
            pic_str = pic_str.replace('V', '')
        elif '.' in pic_str:
            # format actually has a . so we'll parse the 
            precision = len(pic_str[pic_str.index('.') + 1 :])

        return datatype, len(pic_str)+(1 if signed else 0), precision, signed


"""
Represents a copybook hierarchy
"""
class CopyBook:
    # constants
    opt_pattern_format = "({})?"

    row_pattern_base = r'^(?P<level>\d{2})\s+(?P<name>\S+)'
    row_pattern_occurs = r'\s+OCCURS\s+(?P<occurs>\d+)\s*(TIMES)?'
    row_pattern_indexed_by = r"\s+INDEXED BY\s(?P<indexed_by>\S+)"
    row_pattern_redefines = r"\s+REDEFINES\s(?P<redefines>\S+)"
    row_pattern_pic = r'\s+PIC\s+(?P<pic>\S+)'
    row_pattern_comp3 = r'\s+(?P<comp3>COMP-3)'
    row_pattern_signed = r'\s+(SIGN (?P<signed>TRAILING|LEADING) SEPARATE)'
    row_pattern_end = r'\.$'

    row_pattern = re.compile(row_pattern_base + 
                             opt_pattern_format.format(row_pattern_redefines) + 
                             opt_pattern_format.format(row_pattern_occurs) + 
                             opt_pattern_format.format(row_pattern_indexed_by) + 
                             opt_pattern_format.format(row_pattern_pic) + 
                             opt_pattern_format.format(row_pattern_comp3) +
                             opt_pattern_format.format(row_pattern_signed) +
                             row_pattern_end)
    pic_pattern_repeats = re.compile(r'(.)\((\d+)\)')
    pic_pattern_float = re.compile(r'(?P<signed_pic>S|-)?[9Z]*[.V][9Z]+')
    pic_pattern_integer = re.compile(r'(?P<signed_pic>S|-)?[9Z]+')

    def __init__(self):
        self.fields = {}
        self.root:Field = None
        pass

    def get_field_parent(self,field)-> Tuple[Optional[Field],int]:
        # since fields only have children, traverse the tree to find this field's parent. also return index within the children list
        return self._get_field_parent(self.root,field)

    def _get_field_parent(self,root,field_to_find) -> Tuple[Optional[Field],int]:
        try:
            field_index = root._children.index(field_to_find)
            # found
            return root,field_index
        except ValueError:
            # not found. search the children
            for field in root._children:
                field_parent, field_index = self._get_field_parent(field,field_to_find)
                if field_index>-1:
                    return field_parent, field_index
        
        # not found
        return None, -1

    def add_field(
        self,
        field,
        parent_field=None
    ):
        self.fields[field.name] = field
        if (self.root is None):
            self.root = field
        elif parent_field is not None:
            self.fields[parent_field.name].add_child(field)
        else:
            # root item
            self.root.add_child(field)

    def recalculate_lengths_and_offsets(self,root=None,offset=0):
        parent = root
        if parent is None:
            parent = self.root
        total_length = parent.length
        parent.offset = offset
        for field in parent._children:
            if field.redefines is None:
                field_length = self.recalculate_lengths_and_offsets(field,total_length+offset)
                total_length += field_length
            else:
                # if this group redefines another, we don't add its length
                field_length = self.recalculate_lengths_and_offsets(field,self.fields[field.redefines].offset)

        parent.length = total_length
        return parent.length

    @staticmethod
    def parse(contents):
        copybook = CopyBook()

        intify = ["level","occurs"]
        groups = [{"name":'',"level":-1}]
        # parse the lines based on a . at end of line
        lines:List[str] = [line+"." for line in re.split(r'\.\s*\n',contents)]
        for row in lines:
            field:Field = Field.from_copybook_str(row)
            if not field:
                # this is likely an 88 or some other header field
                continue

            if field.level < groups[-1]["level"]:
                # this is a subgroup
                # pop until we reach the same level
                while groups[-1]["level"]>=field.level:
                    groups.pop()
            elif field.level == groups[-1]["level"]:
                groups.pop()

            field.group = groups[-1]["name"]
            copybook.add_field(
                field,
                copybook.fields[field.group] if field.group != '' else None
            )

            groups.append({
                "name":field.name,
                "level":field.level
            })

        # handle those lovely OCCURS. we duplicate them with new names

        for field in copybook.fields.values():
            if field.occurs>1 and field.datatype=="group":
                # find where to add the new objects as siblings
                field_parent, field_index = copybook.get_field_parent(field)
                # duplicate with new name
                for i in range(field.occurs,1,-1):
                    new_root = copy.deepcopy(field)
                    CopyBook._rename_tree(new_root,i)
                    field_parent._children.insert(field_index+1,new_root)

                # rename the original field as field #1
                CopyBook._rename_tree(field,1)

        copybook.recalculate_lengths_and_offsets()
        return copybook

    @staticmethod
    def get_tree(root) -> List[Field]:
        field_list:List[Field] = CopyBook._get_tree(root,[])
        # go over the field list and rename any duplicates
        duplicates:Dict = defaultdict(list)
        for i, field in enumerate(field_list):
            # add index of the field
            duplicates[field.name].append(i)
        for field_name, indexes in sorted(duplicates.items()):
            if len(indexes) >= 2:
                # this is a duplicate
                for dup_index in range(1,len(indexes)):
                    # rename
                    field_list[indexes[dup_index]].name += str(dup_index+1)
        return field_list

    @staticmethod
    def _get_tree(root,fieldlist) -> List[Field]:
        for field in root._children:
            CopyBook._get_tree(field,fieldlist)
            fieldlist.append(field)
        return fieldlist

    @staticmethod
    # add a suffix for an entire tree. used for OCCURS
    def _rename_tree(root,suffix):
        root.name += str(suffix)
        for field in root._children:
            CopyBook._rename_tree(field,suffix)

