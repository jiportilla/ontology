# Purpose
Extract latent taxonomies from unstructured text

## Samples:

1. Use Case 1:
- Input:
`Pharmacology is the branch of pharmaceutical sciences which is concerned with the study of drug or medication action,[1] where a drug can be broadly defined as any man-made, natural, or endogenous (from within the body) molecule which exerts a biochemical or physiological effect on the cell, tissue, organ, or organism (sometimes the word pharmacon is used as a term to encompass these endogenous and exogenous bioactive species).`
- Normalized: `Pharmacology is_the branch of pharmaceutical sciences,`
    1. Transform `['which is']` to a comma
    2. Delimit sentence at comma
- Output:
`Pharmacology -> branch of pharmaceutical sciences`

2. Use Case 2:
- Input: `A pharmaceutical drug, also called a medication or medicine, is a chemical substance used to treat, cure, prevent, or diagnose a disease or to promote well-being`
- Normalized: `A pharmaceutical drug, is_a medication or medicine`
    1. Strip comma after the first `is_a`
    2. Recognize LV of `[medication, medicine]`