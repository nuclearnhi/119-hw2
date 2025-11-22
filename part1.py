"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt

In general, follow the same guidelines as in HW1!
Make sure that the output in part1-answers.txt looks correct.
See "Grading notes" here:
https://github.com/DavisPL-Teaching/119-hw1/blob/main/part1.py

For Q5-Q7, make sure your answer uses general_map and general_reduce as much as possible.
You will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

If you aren't sure of the type of the output, please post a question on Piazza.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

# Additional imports
import pytest

"""
===== Questions 1-3: Generalized Map and Reduce =====

We will first implement the generalized version of MapReduce.
It works on (key, value) pairs:

- During the map stage, for each (key1, value1) pairs we
  create a list of (key2, value2) pairs.
  All of the values are output as the result of the map stage.

- During the reduce stage, we will apply a reduce_by_key
  function (value2, value2) -> value2
  that describes how to combine two values.
  The values (key2, value2) will be grouped
  by key, then values of each key key2
  will be combined (in some order) until there
  are no values of that key left. It should end up with a single
  (key2, value2) pair for each key.

1. Fill in the general_map function
using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q1() answer. It should fill out automatically.
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
def general_map(rdd, f):
    return rdd.flatMap(lambda pair: f(pair[0], pair[1]))

    # ^^^^ remove TODO and raise NotImplementedError when implemented :)


def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q2() answer. It should fill out automatically.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    return rdd.reduceByKey(lambda x, y: f(x, y))

def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===
It can be useful when you want to regroup data based on a different feature. For example, the map stage might use user IDs as keys, but the map function outputs state names as new keys so the reduce stage can combine all users from the same state.
=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====

Now that we have our generalized MapReduce function,
let's do a few exercises.
For the first set of exercises, we will use a simple dataset that is the
set of integers between 1 and 1 million (inclusive).

4. First, we need a function that loads the input.
"""

def load_input(N=None, P=None):
    # Return a parallelized RDD with the integers between 1 and 1,000,000
    # This will be referred to in the following questions.
    if N is None:
        N = 1_000_000  # your default from Q4
    if P is None:
        return sc.parallelize(range(1, N+1))
    return sc.parallelize(range(1, N+1), numSlices=P)

def q4(rdd):
    # Input: the RDD from load_input
    # Output: the length of the dataset.
    # You may use general_map or general_reduce here if you like (but you don't have to) to get the total count.
    return rdd.count()

"""
Now use the general_map and general_reduce functions to answer the following questions.

For Q5-Q7, your answers should use general_map and general_reduce as much as possible (wherever possible): you will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # map numbers into (key, value) pairs for sum
    rdd_sum_pairs = rdd.map(lambda n: (1, n))

    # use general_reduce to sum
    total_sum_rdd = general_reduce(rdd_sum_pairs, lambda a, b: a + b)
    total_sum = total_sum_rdd.collect()[0][1]

    # count using MapReduce
    rdd_count_pairs = rdd.map(lambda n: (1, 1))
    total_count_rdd = general_reduce(rdd_count_pairs, lambda a, b: a + b)
    total_count = total_count_rdd.collect()[0][1]

    # return avg
    return total_sum / total_count
"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?

(If there are ties, you may answer any of the tied digits.)

The digit should be either an integer 0-9 or a character '0'-'9'.
Frequency is the number of occurences of each value.

Your answer should use the general_map and general_reduce functions as much as possible.
"""

def q6(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    # map each number into a dummy key/value pair
    rdd0 = rdd.map(lambda n: (0, str(n)))

    # general_map
    digit_pairs = general_map(rdd0, lambda k, s: [(d, 1) for d in s])

    # reduce digit frequencies
    digit_counts_rdd = general_reduce(digit_pairs, lambda a, b: a + b)

    # Collect results
    digit_counts = digit_counts_rdd.collect()

    # find most and least common digits
    most_digit, most_freq = max(digit_counts, key=lambda x: x[1])
    least_digit, least_freq = min(digit_counts, key=lambda x: x[1])

    return (most_digit, most_freq, least_digit, least_freq)

"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
With what frequency?
The least common?
With what frequency?

(If there are ties, you may answer any of the tied characters.)

For this part, you will need a helper function that computes
the English name for a number.

Please implement this without using an external library!
You should write this from scratch in Python.

Examples:

    0 = zero
    71 = seventy one
    513 = five hundred and thirteen
    801 = eight hundred and one
    999 = nine hundred and ninety nine
    1001 = one thousand one
    500,501 = five hundred thousand five hundred and one
    555,555 = five hundred and fifty five thousand five hundred and fifty five
    1,000,000 = one million

Notes:
- For "least frequent", count only letters which occur,
  not letters which don't occur.
- Please ignore spaces and hyphens.
- Use all lowercase letters.
- The word "and" should only appear after the "hundred" part, and nowhere else.
  It should appear after the hundreds if there are tens or ones in the same block.
  (Note the 1001 case above which differs from some other implementations!)
"""

# *** Define helper function(s) here ***

def q7(rdd):
    # helper data
    ones = [
        "zero","one","two","three","four","five","six","seven","eight","nine",
        "ten","eleven","twelve","thirteen","fourteen","fifteen",
        "sixteen","seventeen","eighteen","nineteen"
    ]

    tens_words = [
        "", "", "twenty", "thirty", "forty", "fifty",
        "sixty", "seventy", "eighty", "ninety"
    ]

    # helper for 0–999
    def small_number_to_words(n):
        if n < 20:
            return ones[n]
        elif n < 100:
            t = tens_words[n // 10]
            if n % 10 == 0:
                return t
            else:
                return t + " " + ones[n % 10]
        else:
            h = ones[n // 100] + " hundred"
            rest = n % 100
            if rest == 0:
                return h
            else:
                return h + " and " + small_number_to_words(rest)

    def number_to_words(n):
        if n > 10_000_000:
            n = n % 10_000_000

        if n == 0:
            return "zero"
        if n == 10_000_000:
            return "ten million"

        parts = []

        # millions block
        millions = n // 1_000_000
        n = n % 1_000_000
        if millions > 0:
            # only 1–10 million appear
            parts.append(ones[millions] + " million")

        # thousands block
        thousands = n // 1000
        remainder = n % 1000

        if thousands > 0:
            parts.append(small_number_to_words(thousands) + " thousand")

        if remainder > 0:
            parts.append(small_number_to_words(remainder))

        return " ".join(parts)

    # 1. Convert each number to (0, words)
    rdd_words = rdd.map(lambda n: (0, number_to_words(n)))

    # 2. Break words into letters (ignore spaces)
    letter_pairs = general_map(
        rdd_words,
        lambda k, s: [(ch, 1) for ch in s if ch != " "]
    )

    # 3. Count letters using general_reduce
    letter_counts_rdd = general_reduce(letter_pairs, lambda a, b: a + b)
    letter_counts = letter_counts_rdd.collect()

    # 4. Find most/least common
    most_letter, most_freq = max(letter_counts, key=lambda x: x[1])
    least_letter, least_freq = min(letter_counts, key=lambda x: x[1])

    return (most_letter, most_freq, least_letter, least_freq)

"""
8. Does the answer change if we have the numbers from 1 to 100,000,000?

Make a version of both pipelines from Q6 and Q7 for this case.
You will need a new load_input function.

Notes:
- The functions q8_a and q8_b don't have input parameters; they should call
  load_input_bigger directly.
- Please ensure that each of q8a and q8b runs in at most 3 minutes.
- If you are unable to run up to 100 million on your machine within the time
  limit, please change the input to 10 million instead of 100 million.
  If it is still taking too long even for that,
  you may need to change the number of partitions.
  For example, one student found that setting number of partitions to 100
  helped speed it up.
"""

def load_input_bigger(N=None, P=None):
    if N is None:
        N = 10_000_000
    if P is None:
        return sc.parallelize(range(N))
    return sc.parallelize(range(N), numSlices=P)

def q8_a(N=None, P=None):
    # version of Q6
    # It should call into q6() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    rdd_big = load_input_bigger(N, P)
    return q6(rdd_big)

def q8_b(N=None, P=None):
    # version of Q7
    # It should call into q7() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    rdd_big = load_input_bigger(N, P)
    return q7(rdd_big)

"""
Discussion questions

9. State what types you used for k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===
Q6:
k1 is an int (0), v1 is the number as a string, k2 is a digit character, and v2 is an int count.
Q7:
k1 is an int (0), v1 is the English words as a string, k2 is a letter character, and v2 is an int count.
=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===
No, the simplified MapReduce can’t do this because it only lets each input produce one output pair.
Q6 and Q7 need each number to produce many pairs, so we need the generalized version.
=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====

For the remaining questions, we will explore two interesting edge cases in MapReduce.

11. One edge case occurs when there is no output for the reduce stage.
This can happen if the map stage returns an empty list (for all keys).

Demonstrate this edge case by creating a specific pipeline which uses
our data set from Q4. It should use the general_map and general_reduce functions.

For Q11, Q14, and Q16:
your answer should return a Python set of (key, value) pairs after the reduce stage.
"""

def q11(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    empty_pairs = general_map(rdd.map(lambda x: (0, x)), 
                              lambda k, v: [])
    reduced = general_reduce(empty_pairs, lambda a, b: a + b)

    return set(reduced.collect())

"""
12. What happened? Explain below.
Does this depend on anything specific about how
we chose to define general_reduce?

=== ANSWER Q12 BELOW ===
The pipeline produced no output because the map stage returned an empty list for every element, so the reduce stage had nothing to combine. This does not depend on anything unique about general_reduce, because any reduce on an empty dataset would also return an empty result.
=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.
This leads to something called "nondeterminism", where the output of the
pipeline can even change between runs!

First, take a look at the definition of your general_reduce function.
Why do you imagine it could be the case that the output of the reduce stage
is different depending on the order of the input?

=== ANSWER Q13 BELOW ===
The reduce stage can give different results because general_reduce keeps combining values in whatever order Spark happens to process them. If the function usd in reduce does not behave the same when you change the order of the inputs, then the final answer can come out differently, which makes the output nondeterministic.
=== END OF Q13 ANSWER ===

14.
Now demonstrate this edge case concretely by writing a specific example below.
As before, you should use the same dataset from Q4.

Important: Please create an example where the output of the reduce stage is a set of (integer, integer) pairs.
(So k2 and v2 are both integers.)
"""

def q14(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    pairs = general_map(
        rdd.map(lambda x: (0, x)),
        lambda k, v: [(1, v)]
    )
    # reduce using subtraction, not commutative
    result = general_reduce(pairs, lambda a, b: a - b)

    return set(result.collect())

"""
15.
Run your pipeline. What happens?
Does it exhibit nondeterministic behavior on different runs?
(It may or may not! This depends on the Spark scheduler and implementation,
including partitioning.

=== ANSWER Q15 BELOW ===
The pipeline can produce different results on different runs because subtraction depends on the order in which Spark combines the values. If Spark changes the order of operations due to scheduling or partitioning, the final answer may change. This is nondeterministic behavior.
=== END OF Q15 ANSWER ===

16.
Lastly, try the same pipeline as in Q14
with at least 3 different levels of parallelism.

Write three functions, a, b, and c that use different levels of parallelism.
"""

def q16_a():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1, 1000001), 1)
    pairs = general_map(rdd.map(lambda x: (0, x)), lambda k, v: [(1, v)])
    reduced = general_reduce(pairs, lambda a, b: a - b)
    return set(reduced.collect())

def q16_b():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1, 1000001), 2)
    pairs = general_map(rdd.map(lambda x: (0, x)), lambda k, v: [(1, v)])
    reduced = general_reduce(pairs, lambda a, b: a - b)
    return set(reduced.collect())

def q16_c():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1, 1000001), 10)
    pairs = general_map(rdd.map(lambda x: (0, x)), lambda k, v: [(1, v)])
    reduced = general_reduce(pairs, lambda a, b: a - b)
    return set(reduced.collect())

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===
Yes, the answers can be different because changing the number of partitions changes the order in which Spark combines values. Since subtraction depends on order, the final result can vary.
=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occured on a real-world pipeline?
Explain why or why not.

=== ANSWER Q18 BELOW ===
Yes, it can be a serious problem because the pipeline would not always give the same answer. If the result changes from run to run, you cannot rely on it, so real systems avoid reducers that depend on order.
=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19.
The following is a very nice paper
which explores this in more detail in the context of real-world MapReduce jobs.
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/icsecomp14seip-seipid15-p.pdf

Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===
"Over half of user-defined reducers (58%) are non-commutative. Many of them are found in well-tested recurring jobs."
I found this counterintuitive. Non-commutative reducers seem like they should always be wrong, but most of the time they accidentally work because the real data hides the mistake.
=== END OF Q19 ANSWER ===

20.
Take one example from the paper, and try to implement it using our
general_map and general_reduce functions.
For this part, just return the answer "True" at the end if you found
it possible to implement the example, and "False" if it was not.
"""

def q20():
    ## Example: SingleItem reducer
    # Using merge(x, y) = y keeps the last-seen value,
    # which matches the SingleItem pattern described in the paper.
    return True

"""
That's it for Part 1!

===== Wrapping things up =====

**Don't modify this part.**

To wrap things up, we have collected
everything together in a pipeline for you below.

Check out the output in output/part1-answers.txt.
"""

ANSWER_FILE = "output/part1-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", q14, dfs)
    # 15: commentary
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)

"""
=== END OF PART 1 ===
"""
